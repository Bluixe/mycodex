"""Operational commands kept separate from the legacy command reference."""
from __future__ import annotations

import json
from pathlib import Path


def emit(value) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def register_commands(sub) -> None:
    doctor = sub.add_parser("doctor", help="Check local dependencies without using credentials")
    doctor.set_defaults(handler=cmd_doctor, no_store=True)
    worker = sub.add_parser("worker", help="Consume persistent jobs")
    worker.add_argument("--once", action="store_true")
    worker.add_argument("--max-jobs", type=int, default=0)
    worker.add_argument("--interval", type=float, default=2)
    worker.add_argument("--job-id", type=int)
    worker.set_defaults(handler=cmd_worker)
    jobs = sub.add_parser("jobs", help="Inspect, cancel and recover durable tasks")
    action = jobs.add_subparsers(dest="jobs_action", required=True)
    p = action.add_parser("list")
    p.add_argument("--project")
    p.set_defaults(handler=cmd_jobs)
    for name in ("show", "cancel", "pause", "resume"):
        p = action.add_parser(name)
        p.add_argument("job_id", type=int)
        p.set_defaults(handler=cmd_jobs)
    p = action.add_parser('revise')
    p.add_argument('job_id', type=int)
    p.add_argument('--prompt', required=True)
    p.add_argument('--revision', type=int, required=True)
    p.set_defaults(handler=cmd_jobs)
    p = action.add_parser("recover")
    p.set_defaults(handler=cmd_jobs)
    p = action.add_parser("submit")
    p.add_argument("--project", required=True)
    from .jobs import JOB_KINDS
    p.add_argument("--kind", choices=sorted(JOB_KINDS), required=True)
    p.add_argument("--payload-json", default="{}")
    p.add_argument("--idempotency-key")
    p.add_argument("--depends-on", action="append", type=int, default=[])
    p.set_defaults(handler=cmd_jobs)
    evidence = sub.add_parser("evidence", help="Inspect versioned source passages and verify claims")
    actions = evidence.add_subparsers(dest="evidence_action", required=True)
    for name in ("search", "list", "versions", "index", "link"):
        p = actions.add_parser(name)
        p.add_argument("--project", required=True)
        p.add_argument("--name", required=True)
        if name == "search":
            p.add_argument("--query", required=True)
        if name in {"versions", "index"}:
            p.add_argument("--source-id", type=int, required=True)
        if name == "index":
            p.add_argument("--file", required=True)
        if name == "link":
            p.add_argument("--claim-id", type=int, required=True)
            p.add_argument("--passage-id", type=int, required=True)
            p.add_argument("--relation", choices=["supports", "contradicts"], default="supports")
            p.add_argument("--verification-status", choices=["unverified", "verified", "rejected"], default="unverified")
            p.add_argument("--verifier", default="")
            p.add_argument("--notes", default="")
        p.set_defaults(handler=cmd_evidence)
    artifacts = sub.add_parser("artifacts", help="Inspect execution manifests or record a result gate")
    actions = artifacts.add_subparsers(dest="artifacts_action", required=True)
    for name in ("list", "result"):
        p = actions.add_parser(name)
        p.add_argument("--project", required=True)
        p.add_argument("--name", required=True)
        if name == "result":
            p.add_argument("--execution-id", required=True)
            p.add_argument("--result-file", required=True)
        p.set_defaults(handler=cmd_artifacts)
    mcp = sub.add_parser("mcp", help="Serve a local STDIO MCP interface")
    mcp.add_argument("--allow-writes", action="store_true", help="Enable bounded job submission/cancellation tools")
    mcp.set_defaults(handler=cmd_mcp)
    evaluations = sub.add_parser("evals", help="Evaluate recorded task state without live model calls")
    evaluations.add_argument("--project")
    evaluations.add_argument("--input", help="JSON file containing a list of exported runs")
    evaluations.set_defaults(handler=cmd_evals)
    reviews = sub.add_parser("reviews", help="Queue bounded independent reviews plus evidence-focused synthesis")
    reviews.add_argument("--project", required=True)
    reviews.add_argument("--prompt", required=True)
    reviews.add_argument("--role", action="append", choices=["evidence", "implementation", "experiment"])
    reviews.add_argument('--risk', action='append', choices=['evidence-conflict','code-change','experiment-design'])
    reviews.add_argument('--policy', choices=['auto','always','never'], default='auto')
    reviews.add_argument("--token-budget", type=int, default=12000)
    reviews.set_defaults(handler=cmd_reviews)
    assessment = sub.add_parser('review-report', help='Inspect review cost or record owner-verified findings')
    assessment.add_argument('workflow_id', type=int)
    assessment.add_argument('--assessment-file')
    assessment.set_defaults(handler=cmd_review_report)
    tracing = sub.add_parser('traces', help='Export metadata-only OTLP/HTTP JSON for one job')
    tracing.add_argument('--job-id', type=int, required=True)
    tracing.add_argument('--output', required=True)
    tracing.add_argument('--endpoint', help='Explicit optional collector endpoint; no automatic network export')
    tracing.set_defaults(handler=cmd_traces)
    benchmarks = sub.add_parser('benchmarks', help='Compare isolated versions on sourced operational cases; no model calls by default')
    benchmarks.add_argument('--output', required=True)
    benchmarks.add_argument('--ref', action='append')
    benchmarks.add_argument('--live-material-pair', action='store_true', help='Explicitly run two read-only model tasks (30000 observed-token budget each)')
    benchmarks.set_defaults(handler=cmd_benchmarks, no_store=True)


def cmd_doctor(_store, _args):
    from .diagnostics import doctor_snapshot
    emit(doctor_snapshot())


def cmd_worker(store, args):
    from .jobs import run_worker
    emit(run_worker(store, once=args.once, max_jobs=args.max_jobs, interval=args.interval, job_id=args.job_id))


def cmd_jobs(store, args):
    from .jobs import JobQueue
    queue = JobQueue(store)
    if args.jobs_action == "submit":
        payload = json.loads(args.payload_json)
        if not isinstance(payload, dict):
            raise ValueError("payload must be a JSON object")
        emit(queue.submit(args.project, args.kind, payload, idempotency_key=args.idempotency_key, depends_on=args.depends_on))
    elif args.jobs_action == "list":
        emit(queue.list(args.project))
    elif args.jobs_action == "recover":
        emit(queue.recover())
    elif args.jobs_action == "cancel":
        emit(queue.cancel(args.job_id))
    elif args.jobs_action == 'pause':
        emit(queue.pause(args.job_id))
    elif args.jobs_action == 'resume':
        emit(queue.resume(args.job_id))
    elif args.jobs_action == 'revise':
        emit(queue.revise(args.job_id, args.prompt, expected_revision=args.revision))
    else:
        job = queue.get(args.job_id)
        if job is None:
            raise ValueError("unknown job")
        emit({"job": job, "events": queue.events(args.job_id)})


def _research(store, args):
    project = store.get_project(args.project)
    research = store.get_research_thread(project["id"], args.name) if project else None
    if research is None:
        raise ValueError("unknown project/research")
    return research


def cmd_evidence(store, args):
    from .research_evidence import EvidenceLedger
    research = _research(store, args)
    ledger = EvidenceLedger(store)
    if args.evidence_action == "search":
        emit(ledger.search_evidence(research["id"], args.query))
    elif args.evidence_action == "versions":
        emit(ledger.get_source_versions(research["id"], args.source_id))
    elif args.evidence_action == "index":
        from .research_connectors import ingest_text_source
        result = ingest_text_source(args.file)
        emit(ledger.index_source(research["id"], args.source_id, text=result["text"],
                                 location=result["resolved_location"], extraction_method=result["method"],
                                 content_hash=result.get("content_hash"), passages=result.get("passages"),
                                 metadata={key: value for key, value in result.items() if key not in {"text", "passages"}}))
    elif args.evidence_action == "link":
        emit(ledger.link_claim_evidence(research["id"], args.claim_id, args.passage_id,
             relation=args.relation, verification_status=args.verification_status, verifier=args.verifier, notes=args.notes))
    else:
        emit(ledger.list_evidence(research["id"]))


def cmd_artifacts(store, args):
    from .experiment_artifacts import list_execution_manifests
    project = store.get_project(args.project)
    experiment = store.get_experiment(project["id"], args.name) if project else None
    if experiment is None:
        raise ValueError("unknown project/experiment")
    if args.artifacts_action == "result":
        from .experiment_ops import record_execution_result
        emit(record_execution_result(store, args.project, args.name, args.execution_id,
                                      json.loads(Path(args.result_file).read_text(encoding="utf-8"))))
    else:
        emit(list_execution_manifests(store.path.parent / "experiment_artifacts", experiment_id=experiment["id"]))


def cmd_mcp(store, args):
    from .mcp_server import serve
    serve(store, allow_writes=args.allow_writes)


def cmd_evals(store, args):
    from .evaluation import evaluate_records
    if args.input:
        records = json.loads(Path(args.input).read_text(encoding="utf-8"))
        if not isinstance(records, list):
            raise ValueError("evaluation input must be a list of run records")
    else:
        if not args.project:
            raise ValueError("--project or --input is required")
        project = store.get_project(args.project)
        if project is None:
            raise ValueError("unknown project")
        records = [dict(row) for row in store.list_runs(project["id"], limit=500)]
    result = evaluate_records(records)
    emit(result)
    if result["failed"]:
        raise ValueError(f"{result['failed']} recorded runs failed evaluation")


def cmd_reviews(store, args):
    from .review_workflow import submit_review
    emit(submit_review(store, args.project, args.prompt, roles=args.role, token_budget=args.token_budget,
                       risks=args.risk, policy=args.policy))


def cmd_review_report(store, args):
    from .review_workflow import workflow_report, record_review_outcome
    if args.assessment_file:
        value = json.loads(Path(args.assessment_file).read_text(encoding='utf-8'))
        if not isinstance(value, dict) or set(value) != {'verifier','findings','notes'}:
            raise ValueError('assessment needs verifier, findings and notes')
        emit(record_review_outcome(store, args.workflow_id, **value))
    else:
        emit(workflow_report(store, args.workflow_id))


def cmd_traces(store, args):
    from .tracing import job_trace, export_trace
    payload = job_trace(store, args.job_id)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2) + '\n', encoding='utf-8')
    result = {'output': str(output.resolve()), 'content': 'Metadata only; prompts, output and source text omitted.'}
    if args.endpoint:
        result['export'] = export_trace(payload, args.endpoint)
    emit(result)


def cmd_benchmarks(_store, args):
    from .benchmarks import run_benchmarks, run_live_material_pair, DEFAULT_REFS
    result = run_benchmarks(Path(__file__).resolve().parents[1], args.output, refs=args.ref or DEFAULT_REFS)
    if args.live_material_pair:
        result['live_material'] = run_live_material_pair(args.output)
    emit(result)
