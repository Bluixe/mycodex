# Procgen Multi-Env ICL Research Handoff

This file is the durable research target for mycodex experiment
`icrl/procgen-multi-env-icl`. It should be injected into future control
turns through the experiment objective, config, and event ledger.

## Accepted Mechanism

- CV-ICRL / auto-relabel works because the context value token acts as a
  task-local competence or phase coordinate, not because raw expected return is
  intrinsically the best value definition.
- The important mapping is: "what competence stage is this policy/context in"
  -> "what action distribution should be imitated or improved".
- Prior ablations already support that the value token itself is useful and
  not merely an auxiliary task artifact. Do not spend future cycles
  re-litigating value-token usefulness or whether competence/phase coordinates
  are effective in principle.

## Research Objective

Improve Procgen held-out multi-env ICL performance while developing a more
novel and defensible method story on top of the accepted competence-coordinate
mechanism.

The next method should explain how context values become a calibrated,
task-local competence coordinate that supports cross-task generalization, not
just tune learning rate, value weight, dropout, or block length for score
gains.

## Non-Goals

- Do not run naive hyperparameter sweeps as the main research loop.
- Do not treat raw expected return matching as the only valid CV-ICRL story if
  it hurts performance.
- Do not block eval data collection because artifact provenance is imperfect;
  record method-claim limitations separately and continue collecting safe eval
  evidence.
- Do not route terminal train completion into repeated peer-review/block loops
  before held-out eval.

## Required State Machine

Use this lifecycle after training completes:

```text
train_completed -> eval_pending -> launch_eval -> result_gate -> next_research_decision
```

`eval-only` and `smoke-eval` are read-only, low-risk execution classes. They
skip peer review and may launch from an explicit pushed commit even if the
local worktree is dirty. Peer review may only block destructive operations,
unauthorized long/paid training, or overwriting/committing unowned local
changes.

## Immediate Resume Policy

When this experiment is resumed, the first control decision should not start a
new full training run blindly. It should:

1. Recover the latest clean checkpoint/run/eval artifacts from the remote
   controller and W&B.
2. Launch or repair an `eval-only` or `smoke-eval` held-out evaluation if the
   current result gate is incomplete.
3. Parse result-gate evidence into one of:
   - `method_claim_allowed=true`: enough artifact evidence for a method claim.
   - `method_claim_allowed=false`: data is useful for triage but not yet a
     clean method claim.
4. Only then propose the next research intervention.

## Preferred Next Hypotheses

Future interventions should be framed as falsifiable method hypotheses. Good
directions include:

- Calibrated competence coordinate: replace raw return tokens with a
  task-local percentile or calibrated competence coordinate while preserving a
  coherent CV-ICRL story.
- Robust value conditioning: train action prediction to remain stable when the
  competence token is missing, perturbed, or uncertain, so the token guides
  phase selection without becoming a brittle shortcut.
- Task-local normalization and transfer: test whether competence coordinates
  normalized within each Procgen task transfer better to unseen envs than raw
  returns or global minmax scaling.
- Reliability-aware context values: attach confidence or reliability to the
  context value estimate and test whether low-confidence contexts should be
  down-weighted or masked.

Each training contract must include a `research` block with hypothesis,
mechanism, baseline, intervention, falsifiable predictions, controls, decision
criteria, analysis plan, and anti-sweep rationale.

## Evaluation Standard

The primary goal is held-out Procgen performance on unseen environments. A run
is not a method success merely because training loss improves or one seen task
improves.

Result gates should separate:

- useful triage evidence,
- clean held-out evidence,
- method-claim evidence,
- and artifact/provenance ambiguity.

Artifact/provenance objections should set `method_claim_allowed=false`; they
must not prevent safe `eval-only` data collection.
