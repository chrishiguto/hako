# Triage Labels

Issues are labelled along **two independent axes**. An issue carries one label from each: who it's for, and whether it's ready. This file maps the canonical roles the skills speak in to the actual label strings used in this repo's tracker.

## Audience — who does the work (a durable property)

| Canonical role | Label in this repo | Meaning                                          |
| -------------- | ------------------ | ------------------------------------------------ |
| `afk`          | `afk`              | An agent can take it end to end, no human needed |
| `hitl`         | `hitl`             | Needs a human in the loop to implement or decide |

## State — is it ready (changes over time)

| Canonical role | Label in this repo | Meaning                                           |
| -------------- | ------------------ | ------------------------------------------------- |
| `needs-triage` | `needs-triage`     | Not yet evaluated — a maintainer needs to sort it |
| `ready`        | `ready`            | Specced and actionable — ready to be picked up    |

The two axes compose: `afk` + `ready` is agent-grabbable now; `hitl` + `ready` is ready but a human owns it; `afk` + `needs-triage` looks like agent work but isn't confirmed yet.

When a skill mentions a role (e.g. "apply the `afk` and `ready` labels"), use the corresponding label strings from these tables. Edit the right-hand columns to match whatever vocabulary your tracker actually uses.
