//! Structural markers for framed prompts. A test that captures a
//! prompt through the sandbox seam asserts a section is present or
//! absent through these — never by spelling the frame's prose — so
//! rewording a section lands in the frame and nowhere else. What rode
//! *inside* a section (a report's summary, a check's output) is the
//! test's own data; assert that directly.

use proto::pipeline::Stage;

use crate::pipeline::{frame, skeptic};
use crate::preamble;

/// The heading that opens the skeptic's prompt — what a fake matches
/// in an exec's argv to tell the skeptic invocation apart from a
/// stage's.
pub const SKEPTIC_PROMPT_HEADING: &str = skeptic::PROMPT_HEADING;

pub fn carries_handoff(prompt: &str) -> bool {
    prompt.contains(frame::HANDOFF_HEADING)
}

pub fn carries_report_from(prompt: &str, stage: Stage) -> bool {
    prompt.contains(&frame::handoff_report_heading(stage))
}

pub fn carries_verify_feedback(prompt: &str) -> bool {
    prompt.contains(preamble::VERIFY_FAILED_HEADING)
}

pub fn carries_human_input(prompt: &str) -> bool {
    prompt.contains(preamble::HUMAN_INPUT_HEADING)
}
