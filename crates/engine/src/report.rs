//! The report vocabulary kernels share — statuses, questions, and
//! answers, uniform across kernels so HITL behaves the same whatever
//! loop is running. They live in `proto` as published language. Each
//! kernel's own report shapes are its dialect (`proto::pipeline` for
//! the pipeline kernel) and are imported by that kernel alone — the
//! engine's shared machinery never speaks them.

pub use proto::report::{Answer, Question, ReportStatus};
