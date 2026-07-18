//! In-tree recording metrics recorder and tracing subscriber for observability tests.
//!
//! Avoids pulling `metrics-util` / `tracing-subscriber` into the library feature so
//! MSRV and consumer builds stay lean. Tests take [`OBS_LOCK`] when installing
//! globals or sharing process-wide state across threads.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use metrics::{
    Counter, CounterFn, Gauge, GaugeFn, Histogram, HistogramFn, Key, KeyName, Metadata, Recorder,
    SharedString, Unit,
};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata as TraceMetadata, Subscriber};
use tracing_core::span::Current;

/// Serializes observability tests that touch process-global or shared recorders.
pub(crate) static OBS_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

pub(crate) fn obs_lock() -> MutexGuard<'static, ()> {
    OBS_LOCK
        .get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

#[derive(Debug, Clone)]
pub(crate) struct MetricSample {
    pub name: String,
    pub value: f64,
    pub labels: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub(crate) struct SpanRecord {
    pub name: String,
    pub attrs: HashMap<String, String>,
    pub errored: bool,
    pub id: u64,
    pub parent_id: Option<u64>,
    meta: &'static TraceMetadata<'static>,
}

#[derive(Debug, Clone)]
pub(crate) struct EventRecord {
    pub name: String,
    pub level: tracing::Level,
    pub fields: HashMap<String, String>,
}

/// Captures every metric emission into a shared sample list.
#[derive(Clone, Default)]
pub(crate) struct RecordingRecorder {
    samples: Arc<Mutex<Vec<MetricSample>>>,
}

impl RecordingRecorder {
    pub(crate) fn new() -> Self {
        RecordingRecorder {
            samples: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(crate) fn snapshot(&self) -> Vec<MetricSample> {
        self.samples
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

fn key_labels(key: &Key) -> Vec<(String, String)> {
    key.labels()
        .map(|l| (l.key().to_string(), l.value().to_string()))
        .collect()
}

struct RecCounter {
    name: String,
    labels: Vec<(String, String)>,
    samples: Arc<Mutex<Vec<MetricSample>>>,
}

impl CounterFn for RecCounter {
    fn increment(&self, value: u64) {
        self.samples
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(MetricSample {
                name: self.name.clone(),
                value: value as f64,
                labels: self.labels.clone(),
            });
    }

    fn absolute(&self, value: u64) {
        self.increment(value);
    }
}

struct RecGauge {
    name: String,
    labels: Vec<(String, String)>,
    samples: Arc<Mutex<Vec<MetricSample>>>,
}

impl GaugeFn for RecGauge {
    fn increment(&self, value: f64) {
        self.set(value);
    }

    fn decrement(&self, value: f64) {
        self.set(-value);
    }

    fn set(&self, value: f64) {
        self.samples
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(MetricSample {
                name: self.name.clone(),
                value,
                labels: self.labels.clone(),
            });
    }
}

struct RecHistogram {
    name: String,
    labels: Vec<(String, String)>,
    samples: Arc<Mutex<Vec<MetricSample>>>,
}

impl HistogramFn for RecHistogram {
    fn record(&self, value: f64) {
        self.samples
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(MetricSample {
                name: self.name.clone(),
                value,
                labels: self.labels.clone(),
            });
    }
}

impl Recorder for RecordingRecorder {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(RecCounter {
            name: key.name().to_string(),
            labels: key_labels(key),
            samples: Arc::clone(&self.samples),
        }))
    }

    fn register_gauge(&self, key: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::from_arc(Arc::new(RecGauge {
            name: key.name().to_string(),
            labels: key_labels(key),
            samples: Arc::clone(&self.samples),
        }))
    }

    fn register_histogram(&self, key: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::from_arc(Arc::new(RecHistogram {
            name: key.name().to_string(),
            labels: key_labels(key),
            samples: Arc::clone(&self.samples),
        }))
    }
}

/// Runs `f` with `rec` as the thread-local metrics recorder.
pub(crate) fn with_metrics<R>(f: impl FnOnce(&RecordingRecorder) -> R) -> R {
    let rec = RecordingRecorder::new();
    metrics::with_local_recorder(&rec, || f(&rec))
}

/// Recording tracing subscriber: captures span names, string attrs, parentage.
pub(crate) struct RecordingSubscriber {
    spans: Arc<Mutex<Vec<SpanRecord>>>,
    events: Arc<Mutex<Vec<EventRecord>>>,
    next_id: Mutex<u64>,
    /// Per-thread enter stack of span ids.
    stack: thread_local::ThreadLocalCell,
}

// Minimal thread-local stack without depending on `thread_local` crate.
mod thread_local {
    use std::cell::RefCell;

    thread_local! {
        static STACK: RefCell<Vec<u64>> = const { RefCell::new(Vec::new()) };
    }

    pub(super) struct ThreadLocalCell;

    impl ThreadLocalCell {
        pub(super) fn push(&self, id: u64) {
            STACK.with(|s| s.borrow_mut().push(id));
        }

        pub(super) fn pop(&self) {
            STACK.with(|s| {
                s.borrow_mut().pop();
            });
        }

        pub(super) fn top(&self) -> Option<u64> {
            STACK.with(|s| s.borrow().last().copied())
        }
    }
}

impl RecordingSubscriber {
    pub(crate) fn new(spans: Arc<Mutex<Vec<SpanRecord>>>) -> Self {
        Self::with_events(spans, Arc::new(Mutex::new(Vec::new())))
    }

    pub(crate) fn with_events(
        spans: Arc<Mutex<Vec<SpanRecord>>>,
        events: Arc<Mutex<Vec<EventRecord>>>,
    ) -> Self {
        RecordingSubscriber {
            spans,
            events,
            next_id: Mutex::new(1),
            stack: thread_local::ThreadLocalCell,
        }
    }
}

struct AttrVisitor<'a> {
    attrs: &'a mut HashMap<String, String>,
    errored: &'a mut bool,
}

impl Visit for AttrVisitor<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "error.sqlstate" {
            // Presence of the field means failure — including empty SQLSTATE on
            // pool timeouts (C++ setStatusError with "").
            *self.errored = true;
        }
        self.attrs
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        let s = format!("{value:?}");
        // Strip surrounding quotes from Debug of &str.
        let cleaned = s
            .strip_prefix('"')
            .and_then(|t| t.strip_suffix('"'))
            .unwrap_or(&s)
            .to_string();
        if field.name() == "error.sqlstate" {
            *self.errored = true;
        }
        self.attrs.insert(field.name().to_string(), cleaned);
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.attrs
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.attrs
            .insert(field.name().to_string(), value.to_string());
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.attrs
            .insert(field.name().to_string(), value.to_string());
    }
}

impl Subscriber for RecordingSubscriber {
    fn enabled(&self, _: &TraceMetadata<'_>) -> bool {
        true
    }

    fn new_span(&self, span: &Attributes<'_>) -> Id {
        let mut next = self.next_id.lock().unwrap_or_else(|e| e.into_inner());
        let id = *next;
        *next += 1;
        drop(next);

        let parent_id = span
            .parent()
            .map(|p| p.into_u64())
            .or_else(|| self.stack.top());

        let mut attrs = HashMap::new();
        let mut errored = false;
        span.record(&mut AttrVisitor {
            attrs: &mut attrs,
            errored: &mut errored,
        });

        self.spans
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(SpanRecord {
                name: span.metadata().name().to_string(),
                attrs,
                errored,
                id,
                parent_id,
                meta: span.metadata(),
            });

        Id::from_u64(id)
    }

    fn record(&self, span: &Id, values: &Record<'_>) {
        let mut guard = self.spans.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(rec) = guard.iter_mut().find(|s| s.id == span.into_u64()) {
            values.record(&mut AttrVisitor {
                attrs: &mut rec.attrs,
                errored: &mut rec.errored,
            });
        }
    }

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let mut fields = HashMap::new();
        let mut errored = false;
        event.record(&mut AttrVisitor {
            attrs: &mut fields,
            errored: &mut errored,
        });
        self.events
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(EventRecord {
                name: event.metadata().name().to_string(),
                level: *event.metadata().level(),
                fields,
            });

        // Mark the current span errored on error-level events.
        if *event.metadata().level() <= tracing::Level::ERROR {
            if let Some(top) = self.stack.top() {
                let mut guard = self.spans.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(rec) = guard.iter_mut().find(|s| s.id == top) {
                    rec.errored = true;
                    event.record(&mut AttrVisitor {
                        attrs: &mut rec.attrs,
                        errored: &mut rec.errored,
                    });
                }
            }
        }
    }

    fn enter(&self, span: &Id) {
        self.stack.push(span.into_u64());
    }

    fn exit(&self, _: &Id) {
        self.stack.pop();
    }

    fn current_span(&self) -> Current {
        match self.stack.top() {
            Some(id) => {
                let guard = self.spans.lock().unwrap_or_else(|e| e.into_inner());
                if let Some(rec) = guard.iter().find(|s| s.id == id) {
                    Current::new(Id::from_u64(id), rec.meta)
                } else {
                    Current::none()
                }
            }
            None => Current::none(),
        }
    }
}

/// Runs `f` with a recording subscriber as the thread-local default.
pub(crate) fn with_tracing<R>(f: impl FnOnce(Arc<Mutex<Vec<SpanRecord>>>) -> R) -> R {
    let spans = Arc::new(Mutex::new(Vec::new()));
    let sub = RecordingSubscriber::new(Arc::clone(&spans));
    tracing::subscriber::with_default(sub, || f(spans))
}

/// Runs `f` with independently recorded spans and events.
pub(crate) fn with_tracing_events<R>(
    f: impl FnOnce(Arc<Mutex<Vec<SpanRecord>>>, Arc<Mutex<Vec<EventRecord>>>) -> R,
) -> R {
    let spans = Arc::new(Mutex::new(Vec::new()));
    let events = Arc::new(Mutex::new(Vec::new()));
    let sub = RecordingSubscriber::with_events(Arc::clone(&spans), Arc::clone(&events));
    tracing::subscriber::with_default(sub, || f(spans, events))
}

/// Runs `f` with both a recording metrics recorder and tracing subscriber.
pub(crate) fn with_obs<R>(
    f: impl FnOnce(&RecordingRecorder, Arc<Mutex<Vec<SpanRecord>>>) -> R,
) -> R {
    with_metrics(|rec| with_tracing(|spans| f(rec, spans)))
}

pub(crate) fn labels_match(have: &[(String, String)], want: &[(&str, &str)]) -> bool {
    want.iter()
        .all(|(k, v)| have.iter().any(|(hk, hv)| hk == k && hv == v))
}

pub(crate) fn count(samples: &[MetricSample], name: &str, labels: &[(&str, &str)]) -> usize {
    samples
        .iter()
        .filter(|s| s.name == name && labels_match(&s.labels, labels))
        .count()
}

pub(crate) fn max_value(samples: &[MetricSample], name: &str, labels: &[(&str, &str)]) -> f64 {
    samples
        .iter()
        .filter(|s| s.name == name && labels_match(&s.labels, labels))
        .map(|s| s.value)
        .fold(-1.0_f64, f64::max)
}

pub(crate) fn find_span<'a>(spans: &'a [SpanRecord], name: &str) -> Option<&'a SpanRecord> {
    spans.iter().find(|s| s.name == name)
}

pub(crate) fn find_child<'a>(
    spans: &'a [SpanRecord],
    name: &str,
    parent_id: u64,
) -> Option<&'a SpanRecord> {
    spans
        .iter()
        .find(|s| s.name == name && s.parent_id == Some(parent_id))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recorder_captures_counter() {
        let _g = obs_lock();
        with_metrics(|rec| {
            metrics::counter!("halcyon_queries_total", "op" => "insert", "status" => "ok")
                .increment(1);
            assert_eq!(
                count(
                    &rec.snapshot(),
                    "halcyon_queries_total",
                    &[("op", "insert"), ("status", "ok")]
                ),
                1
            );
        });
    }

    #[test]
    fn subscriber_captures_span_name() {
        let _g = obs_lock();
        with_tracing(|spans| {
            let span = tracing::info_span!("halcyon.execute", db.system = "db2");
            let _e = span.enter();
            let guard = spans.lock().unwrap();
            let s = find_span(&guard, "halcyon.execute").expect("span");
            assert_eq!(s.attrs.get("db.system").map(String::as_str), Some("db2"));
        });
    }

    #[test]
    fn subscriber_records_events_independently_from_spans() {
        let _g = obs_lock();
        with_tracing_events(|spans, events| {
            tracing::event!(
                name: "connect.fail",
                tracing::Level::ERROR,
                attempt = 2_u64,
                code = "connection",
                sqlstate = "08001"
            );

            assert!(spans.lock().unwrap().is_empty());
            let guard = events.lock().unwrap();
            let event = guard.first().expect("event");
            assert_eq!(event.name, "connect.fail");
            assert_eq!(event.level, tracing::Level::ERROR);
            assert_eq!(event.fields.get("attempt").map(String::as_str), Some("2"));
            assert_eq!(
                event.fields.get("code").map(String::as_str),
                Some("connection")
            );
            assert_eq!(
                event.fields.get("sqlstate").map(String::as_str),
                Some("08001")
            );
        });
    }

    #[test]
    fn connect_fail_event_has_exact_catalog_metadata() {
        let _g = obs_lock();
        with_tracing_events(|_, events| {
            let error = crate::error::Error {
                code: crate::error::ErrorCode::Connection,
                sqlstate: "08001".into(),
                native_error: -30082,
                message: "connect failed".into(),
                retriable: true,
            };
            crate::obs::emit_connect_fail(3, &error);

            let guard = events.lock().unwrap();
            let event = guard.first().expect("event");
            assert_eq!(event.name, "connect.fail");
            assert_eq!(event.level, tracing::Level::ERROR);
            assert_eq!(event.fields.len(), 3);
            assert_eq!(event.fields.get("attempt").map(String::as_str), Some("3"));
            assert_eq!(
                event.fields.get("code").map(String::as_str),
                Some("Connection")
            );
            assert_eq!(
                event.fields.get("sqlstate").map(String::as_str),
                Some("08001")
            );
        });
    }
}
