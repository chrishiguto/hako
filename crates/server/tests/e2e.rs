#![cfg(target_os = "linux")]

#[path = "e2e/support.rs"]
mod support;

#[test]
#[ignore = "requires KVM, smolvm, an agent image, network, and a provisioned secret"]
fn hako_run_reaches_verified_done_on_real_infrastructure() {
    support::run();
}
