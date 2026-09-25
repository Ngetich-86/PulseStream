//! The bounded in-memory event pipeline.
//!
//! ```text
//! Admission::try_admit ──try_send──▶ mpsc(queue_capacity) ──▶ dispatcher ──spawn──▶ JoinSet (≤ worker_concurrency tasks)
//! ```
//!
//! Boundedness, in the terms of ADR-004 and ADR-006:
//!
//! - Admission never waits: a full queue is reported as [`AdmitError::Full`].
//! - The dispatcher is the queue's only consumer. It dequeues an event only
//!   while its `JoinSet` holds fewer than `worker_concurrency` tasks, and a
//!   task leaves the set only after the dispatcher has joined it. Tasks that
//!   exist (running or finished but not yet joined) therefore never exceed
//!   `worker_concurrency`, so neither do concurrently processed events. At
//!   most `queue_capacity + worker_concurrency` events are held in memory.
//! - Admission is process-local and non-durable. A crash loses events that
//!   were admitted but not yet processed. Durable acceptance arrives in M2.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};

use pulsestream_core::config::PipelineConfig;
use pulsestream_core::event::{Event, EventId};
use tokio::sync::{mpsc, oneshot};
use tokio::task::{JoinError, JoinHandle, JoinSet};
use tracing::{error, info, warn};

/// A processing failure. The message is logged. M1 does not retry (M3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProcessError(pub String);

impl std::fmt::Display for ProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ProcessError {}

/// Handles one admitted event.
///
/// This is a trait so tests can substitute processors that block on demand
/// and record concurrency. The pipeline, not the processor, enforces the
/// concurrency bound.
pub trait EventProcessor: Send + Sync + 'static {
    fn process(&self, event: &Event) -> impl Future<Output = Result<(), ProcessError>> + Send;
}

/// The M1 default processor. It performs no business action. Completion is
/// recorded in the pipeline's lifecycle logs only.
#[derive(Debug, Default, Clone, Copy)]
pub struct AcknowledgeProcessor;

impl EventProcessor for AcknowledgeProcessor {
    async fn process(&self, _event: &Event) -> Result<(), ProcessError> {
        Ok(())
    }
}

/// Why an event was not admitted. The event is dropped and was never queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmitError {
    /// The queue is at capacity. The caller should retry later.
    Full,
    /// The pipeline is shutting down or has stopped.
    Closed,
}

#[derive(Debug, Default)]
struct Counters {
    in_flight: AtomicUsize,
    processed: AtomicU64,
    failed: AtomicU64,
}

/// Cloneable, non-blocking admission handle given to the API.
#[derive(Debug, Clone)]
pub struct Admission {
    sender: mpsc::Sender<Event>,
    closed: Arc<AtomicBool>,
}

impl Admission {
    /// Admits `event` if there is queue capacity. Never waits.
    pub fn try_admit(&self, event: Event) -> Result<(), AdmitError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(AdmitError::Closed);
        }
        self.sender.try_send(event).map_err(|err| match err {
            mpsc::error::TrySendError::Full(_) => AdmitError::Full,
            mpsc::error::TrySendError::Closed(_) => AdmitError::Closed,
        })
    }

    /// Stops admitting new events. Already-admitted events still drain.
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }

    /// True while events can be admitted: admission is open and the
    /// dispatcher is still consuming the queue.
    pub fn is_available(&self) -> bool {
        !self.closed.load(Ordering::Acquire) && !self.sender.is_closed()
    }

    /// Number of admitted events waiting for a worker.
    pub fn queued(&self) -> usize {
        self.sender.max_capacity() - self.sender.capacity()
    }
}

/// Outcome of [`Pipeline::shutdown`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DrainReport {
    pub processed: u64,
    pub failed: u64,
    /// True if the timeout expired and remaining work was abandoned.
    pub timed_out: bool,
    /// Events that were being processed when work was abandoned.
    pub abandoned_in_flight: usize,
    /// Events still queued when work was abandoned.
    pub abandoned_queued: usize,
}

/// Owner of the running pipeline.
#[derive(Debug)]
pub struct Pipeline {
    admission: Admission,
    counters: Arc<Counters>,
    stop: Option<oneshot::Sender<()>>,
    dispatcher: JoinHandle<()>,
}

impl Pipeline {
    /// Starts the dispatcher. It must be called within a Tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics if `queue_capacity` or `worker_concurrency` is zero. Validated
    /// configuration never contains zero.
    pub fn start<P: EventProcessor>(config: &PipelineConfig, processor: P) -> Self {
        assert!(config.queue_capacity > 0, "queue_capacity must be > 0");
        assert!(
            config.worker_concurrency > 0,
            "worker_concurrency must be > 0"
        );

        let (sender, receiver) = mpsc::channel(config.queue_capacity);
        let (stop_tx, stop_rx) = oneshot::channel();
        let counters = Arc::new(Counters::default());
        let dispatcher = tokio::spawn(dispatch(
            receiver,
            stop_rx,
            config.worker_concurrency,
            Arc::new(processor),
            Arc::clone(&counters),
        ));
        info!(
            queue_capacity = config.queue_capacity,
            worker_concurrency = config.worker_concurrency,
            max_occupancy = config.max_occupancy(),
            "event pipeline started"
        );
        Self {
            admission: Admission {
                sender,
                closed: Arc::new(AtomicBool::new(false)),
            },
            counters,
            stop: Some(stop_tx),
            dispatcher,
        }
    }

    pub fn admission(&self) -> Admission {
        self.admission.clone()
    }

    /// Events currently being processed.
    pub fn in_flight(&self) -> usize {
        self.counters.in_flight.load(Ordering::Acquire)
    }

    /// Stops admission, drains queued and in-flight events, and waits for
    /// all processing to finish, for at most `timeout`. Anything still running
    /// after that is aborted and reported as abandoned.
    pub async fn shutdown(mut self, timeout: Duration) -> DrainReport {
        self.admission.close();
        if let Some(stop) = self.stop.take() {
            // An Err only means the dispatcher already exited.
            let _ = stop.send(());
        }

        let timed_out = tokio::time::timeout(timeout, &mut self.dispatcher)
            .await
            .is_err();
        let (abandoned_in_flight, abandoned_queued) = if timed_out {
            let in_flight = self.in_flight();
            let queued = self.admission.queued();
            // Dropping the dispatcher drops its JoinSet, which aborts every
            // processing task.
            self.dispatcher.abort();
            let _ = (&mut self.dispatcher).await;
            error!(
                timeout_ms = u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
                abandoned_in_flight = in_flight,
                abandoned_queued = queued,
                "shutdown drain timed out; remaining events abandoned"
            );
            (in_flight, queued)
        } else {
            (0, 0)
        };

        DrainReport {
            processed: self.counters.processed.load(Ordering::Acquire),
            failed: self.counters.failed.load(Ordering::Acquire),
            timed_out,
            abandoned_in_flight,
            abandoned_queued,
        }
    }
}

/// The single consumer of the admission queue.
async fn dispatch<P: EventProcessor>(
    mut receiver: mpsc::Receiver<Event>,
    mut stop: oneshot::Receiver<()>,
    worker_concurrency: usize,
    processor: Arc<P>,
    counters: Arc<Counters>,
) {
    let mut tasks = JoinSet::new();
    // Maps running tasks to their event, so a panic can be attributed.
    // Holds at most `worker_concurrency` entries.
    let mut running: HashMap<tokio::task::Id, EventId> = HashMap::new();
    let mut draining = false;

    loop {
        reap_finished(&mut tasks, &mut running, &counters);

        // Wait for a free slot *before* dequeuing, so an event leaves the
        // queue only when it can start processing immediately.
        if tasks.len() >= worker_concurrency {
            tokio::select! {
                biased;
                _ = &mut stop, if !draining => {
                    draining = true;
                    begin_drain(&mut receiver);
                }
                Some(result) = tasks.join_next_with_id() => {
                    record_join(result, &mut running, &counters);
                }
            }
            continue;
        }

        let event = tokio::select! {
            biased;
            _ = &mut stop, if !draining => {
                draining = true;
                begin_drain(&mut receiver);
                continue;
            }
            event = receiver.recv() => event,
        };
        let Some(event) = event else {
            // The queue is closed and empty.
            break;
        };

        let processor = Arc::clone(&processor);
        let task_counters = Arc::clone(&counters);
        let event_id = event.id;
        let handle = tasks.spawn(async move {
            process_one(processor.as_ref(), &event, &task_counters).await;
        });
        running.insert(handle.id(), event_id);
    }

    while let Some(result) = tasks.join_next_with_id().await {
        record_join(result, &mut running, &counters);
    }
    info!(
        processed = counters.processed.load(Ordering::Acquire),
        failed = counters.failed.load(Ordering::Acquire),
        "shutdown drain complete"
    );
}

fn begin_drain(receiver: &mut mpsc::Receiver<Event>) {
    // Closing the receiver rejects further sends but keeps buffered events
    // available to `recv`.
    receiver.close();
    info!(
        queued = receiver.len(),
        "shutdown drain started; admission closed"
    );
}

fn reap_finished(
    tasks: &mut JoinSet<()>,
    running: &mut HashMap<tokio::task::Id, EventId>,
    counters: &Counters,
) {
    while let Some(result) = tasks.try_join_next_with_id() {
        record_join(result, running, counters);
    }
}

fn record_join(
    result: Result<(tokio::task::Id, ()), JoinError>,
    running: &mut HashMap<tokio::task::Id, EventId>,
    counters: &Counters,
) {
    match result {
        Ok((id, ())) => {
            running.remove(&id);
        }
        Err(err) => {
            let event_id = running.remove(&err.id());
            if err.is_panic() {
                counters.failed.fetch_add(1, Ordering::AcqRel);
                // The in-flight guard in `process_one` already ran during unwinding.
                error!(
                    event_id = event_id.map(|id| id.to_string()),
                    state = "failed",
                    "event processing failed: processor panicked"
                );
            }
        }
    }
}

/// Decrements `in_flight` on drop, including when the processor panics.
struct InFlight<'a>(&'a AtomicUsize);

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

async fn process_one<P: EventProcessor>(processor: &P, event: &Event, counters: &Counters) {
    counters.in_flight.fetch_add(1, Ordering::AcqRel);
    let _in_flight = InFlight(&counters.in_flight);
    let queued_ms = SystemTime::now()
        .duration_since(event.accepted_at)
        .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    info!(
        event_id = %event.id,
        event_type = event.event_type.as_str(),
        source = event.source.as_str(),
        state = "processing",
        queued_ms,
        "event processing started"
    );

    match processor.process(event).await {
        Ok(()) => {
            counters.processed.fetch_add(1, Ordering::AcqRel);
            info!(
                event_id = %event.id,
                event_type = event.event_type.as_str(),
                source = event.source.as_str(),
                state = "processed",
                "event processing completed"
            );
        }
        Err(err) => {
            counters.failed.fetch_add(1, Ordering::AcqRel);
            warn!(
                event_id = %event.id,
                event_type = event.event_type.as_str(),
                source = event.source.as_str(),
                state = "failed",
                error = %err,
                "event processing failed; not retried in M1"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{GatedProcessor, event};

    fn config(queue_capacity: usize, worker_concurrency: usize) -> PipelineConfig {
        PipelineConfig {
            queue_capacity,
            worker_concurrency,
            shutdown_timeout: Duration::from_secs(5),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrency_never_exceeds_configured_bound() {
        let processor = GatedProcessor::new();
        let pipeline = Pipeline::start(&config(64, 4), processor.clone());
        let admission = pipeline.admission();
        for _ in 0..20 {
            admission.try_admit(event()).unwrap();
        }

        // Exactly four start; the rest wait in the queue while the gate is shut.
        processor.wait_started(4).await;
        for _ in 0..100 {
            tokio::task::yield_now().await;
        }
        assert_eq!(processor.started(), 4);
        assert_eq!(processor.current(), 4);
        assert_eq!(pipeline.in_flight(), 4);
        assert_eq!(admission.queued(), 16);

        processor.release(20);
        let report = pipeline.shutdown(Duration::from_secs(5)).await;
        assert_eq!(report.processed, 20);
        assert_eq!(processor.max_observed(), 4);
        assert_eq!(processor.completed(), 20);
    }

    #[tokio::test]
    async fn full_queue_rejects_without_dropping_admitted_events() {
        let processor = GatedProcessor::new();
        let pipeline = Pipeline::start(&config(2, 1), processor.clone());
        let admission = pipeline.admission();

        let first = event();
        let first_id = first.id;
        admission.try_admit(first).unwrap();
        processor.wait_started(1).await; // the one worker slot is now occupied
        let queued: Vec<_> = (0..2).map(|_| event()).collect();
        let queued_ids: Vec<_> = queued.iter().map(|e| e.id).collect();
        for e in queued {
            admission.try_admit(e).unwrap();
        }

        let rejected = event();
        let rejected_id = rejected.id;
        assert_eq!(admission.try_admit(rejected), Err(AdmitError::Full));

        processor.release(3);
        let report = pipeline.shutdown(Duration::from_secs(5)).await;
        assert_eq!(report.processed, 3);
        let seen = processor.seen();
        assert_eq!(seen, vec![first_id, queued_ids[0], queued_ids[1]]);
        assert!(!seen.contains(&rejected_id));
    }

    #[tokio::test]
    async fn shutdown_drains_admitted_events_and_rejects_new_ones() {
        let processor = GatedProcessor::new();
        let pipeline = Pipeline::start(&config(8, 2), processor.clone());
        let admission = pipeline.admission();
        for _ in 0..6 {
            admission.try_admit(event()).unwrap();
        }
        processor.wait_started(2).await;

        let drain = tokio::spawn(pipeline.shutdown(Duration::from_secs(5)));
        // Admission closes synchronously at the start of shutdown.
        while admission.is_available() {
            tokio::task::yield_now().await;
        }
        assert_eq!(admission.try_admit(event()), Err(AdmitError::Closed));

        processor.release(6);
        let report = drain.await.unwrap();
        assert_eq!(
            report,
            DrainReport {
                processed: 6,
                failed: 0,
                timed_out: false,
                abandoned_in_flight: 0,
                abandoned_queued: 0,
            }
        );
        assert_eq!(processor.completed(), 6);
        assert!(admission.sender.is_closed(), "dispatcher must have exited");
    }

    #[tokio::test]
    async fn shutdown_times_out_and_abandons_stuck_work() {
        let processor = GatedProcessor::new(); // never released
        let pipeline = Pipeline::start(&config(8, 2), processor.clone());
        let admission = pipeline.admission();
        for _ in 0..5 {
            admission.try_admit(event()).unwrap();
        }
        processor.wait_started(2).await;

        let report = pipeline.shutdown(Duration::from_millis(50)).await;
        assert!(report.timed_out);
        assert_eq!(report.processed, 0);
        assert_eq!(report.abandoned_in_flight, 2);
        assert_eq!(report.abandoned_queued, 3);
        assert_eq!(processor.completed(), 0);
        assert!(admission.sender.is_closed());
    }

    #[tokio::test]
    async fn failures_and_panics_are_counted_and_do_not_stop_processing() {
        struct Flaky;
        impl EventProcessor for Flaky {
            async fn process(&self, event: &Event) -> Result<(), ProcessError> {
                match event.event_type.as_str() {
                    "fail" => Err(ProcessError("boom".into())),
                    "panic" => panic!("processor bug"),
                    _ => Ok(()),
                }
            }
        }
        let pipeline = Pipeline::start(&config(8, 1), Flaky);
        let admission = pipeline.admission();
        for kind in ["ok", "fail", "panic", "ok"] {
            let e = Event::new("test", kind, serde_json::json!({})).unwrap();
            admission.try_admit(e).unwrap();
        }
        let report = pipeline.shutdown(Duration::from_secs(5)).await;
        assert_eq!((report.processed, report.failed), (2, 2));
        assert!(!report.timed_out);
    }

    // Single-threaded runtime on purpose: there `num_alive_tasks` is exact.
    // On the multi-threaded runtime a joined task is released by its worker
    // thread slightly later, so the metric can briefly overcount.
    #[tokio::test]
    async fn many_events_complete_with_bounded_tasks() {
        // Not a benchmark: checks that every admitted event completes, the
        // bound holds, and the task count never exceeds dispatcher + concurrency.
        let processor = GatedProcessor::open();
        let pipeline = Pipeline::start(&config(256, 4), processor.clone());
        let admission = pipeline.admission();
        let baseline_tasks = tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks();

        let mut admitted = 0;
        let mut peak_tasks = 0;
        while admitted < 2_000 {
            match admission.try_admit(event()) {
                Ok(()) => admitted += 1,
                Err(AdmitError::Full) => tokio::task::yield_now().await,
                Err(AdmitError::Closed) => panic!("pipeline closed unexpectedly"),
            }
            let alive = tokio::runtime::Handle::current()
                .metrics()
                .num_alive_tasks();
            peak_tasks = peak_tasks.max(alive);
        }

        let report = pipeline.shutdown(Duration::from_secs(10)).await;
        assert_eq!(report.processed, 2_000);
        assert_eq!(processor.completed(), 2_000);
        assert!(processor.max_observed() <= 4);
        // Only the (already running) dispatcher plus at most four processing tasks.
        assert!(
            peak_tasks <= baseline_tasks + 4,
            "peak {peak_tasks}, baseline {baseline_tasks}"
        );
    }
}
