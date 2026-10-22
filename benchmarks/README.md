# Reproducible behavioral and source-grounding cases

Run `python -m mycodex.benchmarks --output data/benchmark-UNIQUE` from the checkout.
The default comparison snapshots public baseline `ed16415` and the working tree.
Add `--ref ed16415 --ref a4eb8d0 --ref working-tree` to include the local first
iteration commit when it is available. Unavailable revisions are `missing_ref`
with unknown results, never a pass. The
same interpreter, adapter, inputs and temporary-database layout are used in fresh
processes. Snapshots include source-tree hashes. No tracked checkout is switched.
Outputs, fixture databases, logs and source archives belong under ignored `data/`.

## Provenance and scope

- `observed_historical_failure` means a retained original incident trace with a
  hash. This checkout currently contains **zero** such traces. A prior audit's
  descriptions are not relabeled as original production evidence.
- `constructed_from_incident` means a deliberately built input based on a known
  failure class. All deterministic cases currently have this label. Exceptions,
  token events, reviewer verification and concurrent execution interleavings are
  injected. This is actual execution of the version's code on constructed inputs.
- `live_material_task` means a real model turn over pinned official source
  materials. Such turns are opt-in and have separate runtime/usage/archive
  receipts; deterministic retrieval cases must never receive this label.

Missing features are `unsupported`, excluded from the check-pass denominator.
The report also lists cases evaluable in every compared version. Shared positive
controls allow older versions to pass. No adapter branches on a version name or
asserts that an older revision must fail.

The lease race synchronizes the two separate SELECT results only if that actual
code path exists; atomic UPDATE implementations use the same start barrier but
have no separate select interleaving. No SSH job is launched. Recovery cases
write fixture remote events, not real processes. The review test checks a finite
DAG and allocation, not live review quality or real recursive-agent behavior.

## Official source material task

`materials/sqlite.json` holds short extracts from SQLite's official public-domain
documentation, source URLs, access dates, original response hashes, excerpt
hashes and checkable gold answers. Raw fetched responses are archived only in
the dated ignored benchmark output. The gold is intentionally about scope and
exceptions: one writer under WAL, compile-time mutex removal, consistent online
backup snapshots, and an unsupported universal performance assertion.

The retrieval stress case stores the same three extracts plus 18 later unrelated
sources in each version's database, then uses the version's default control
context. It measures supplied evidence coverage, not model accuracy. Any later
model comparison must disclose this context difference, keep the model/runtime
and questions fixed, count abstentions separately from wrong answers, and verify
citations against the **visible** source packet. It cannot establish a causal
improvement in general research quality from one pair of turns.

## Metrics

Harness/behavior scorer version 2 distinguishes server completion from queue
permission to continue. The `token-budget` case now runs the actual queue,
service and native wait on identical constructed events (25 observed tokens,
budget 20, followed by a real completed server event). It requires a non-success
queue state and a blocked dependent task. The separate
`completion-race-positive` case requires native transport to preserve a genuine
server completion and its answer, while recording the interrupt request. A
server can finish before interruption takes effect; suppressing that truth is
not budget enforcement. Previous 12/13 reports are retained unchanged and are
not comparable to the new 14-case denominator without this contract note.

Add `--gate-ref working-tree` to either benchmark CLI to get a failing exit code
when the selected revision is missing, unknown, empty, unsupported or has any
failed/error case. Expected failures of another selected baseline do not fail
this target-specific gate. The Python API is `gate_report(report, ref)`.

Report actual adapter runtime and declared completion/error completion when
observable. Constructed cases use zero live inference tokens and zero observed
fixture interactions; expected production cost or human intervention is unknown.
Missing usage is never filled with zero. Version timings are one local sample,
not statistically meaningful speed measurements. Evidence-graph receipts check
state integrity; human semantic verification remains a separate measurement.

## Explicit live option and bounds

Normal benchmark commands never start a model. Adding `--live-material-pair`
explicitly requests two real read-only turns after the comparison. The default
quality phase requests at most 30,000 observed total tokens and 180 seconds per
turn. It uses isolated temporary working directories, never asks the model to use
tools, and archives both test threads. It compares the oldest and newest selected
contexts through the same current native transport. It does not benchmark an old
protocol client against a new one.

The API `run_live_material_pair(report_dir, token_budget=30000, timeout=180,
phase="quality_probe")` has the same bounds. A separately requested
`phase="budget_probe"` permits at most 8,000 tokens per turn. Each phase can be
run only once per report directory; at most four real attempts across phases are
allowed. Usage-event interruption can happen after initial input tokens have
already been charged, so this is not a hard provider-side billing limit.

`rescore_live_material(report_dir)` evaluates saved answers without inference and
writes a separate scores file; it never overwrites raw model receipts. The first
local quality pair exposed and fixed an exact-quote scorer bug involving escaped
newlines. Corrected scoring preserves that history. This example's old context
appropriately abstained when evidence was absent; low answer coverage must not
be mislabeled as hallucination.
