//! The report vocabulary kernels share. The statuses, questions, and
//! answers live in `proto` — uniform across kernels so HITL behaves
//! the same whatever loop is running — and so do the pipeline's stage
//! report shapes: they are wire contract, validated on every side.

pub use proto::report::{
    Answer, DeliverReport, ImplementReport, PlanReport, Question, ReportStatus, ReviewReport,
    SimplifyReport, Stage, StageReport,
};
