//! The report vocabulary kernels share — statuses, summaries,
//! questions, and answers, uniform across kernels so HITL behaves the
//! same whatever loop is running, and so dialect-blind machinery can
//! read any kernel's report through [`ReportCore`]. They live in
//! `proto` as published language. Each kernel's own report shapes are
//! its dialect (`proto::pipeline` for the pipeline kernel) and are
//! imported by that kernel alone — the engine's shared machinery
//! never speaks them.

pub use proto::report::{Answer, Question, ReportCore, ReportStatus};

use crate::run::PauseReason;

/// Where a report's status sends the run — the single mapping from
/// the shared status vocabulary to kernel control flow, so no kernel
/// respells it.
pub(crate) enum Disposition {
    /// `continue`: the work advances within the run.
    Advance,
    /// `done`: a completion claim, to be verified and judged before
    /// it ends anything.
    Claimed,
    /// `blocked` / `needs_input`: the run pauses for this reason.
    Pause(PauseReason),
}

pub(crate) fn disposition(status: ReportStatus) -> Disposition {
    match status {
        ReportStatus::Continue => Disposition::Advance,
        ReportStatus::Done => Disposition::Claimed,
        ReportStatus::Blocked => Disposition::Pause(PauseReason::Blocked),
        ReportStatus::NeedsInput => Disposition::Pause(PauseReason::AwaitingHuman),
    }
}
