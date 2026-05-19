from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class TopicProfile:
    key: str
    label: str
    positive: tuple[str, ...]
    required_any: tuple[str, ...]
    negative: tuple[str, ...]
    canonical_queries: tuple[str, ...]
    stable_claims: tuple[dict[str, str], ...]
    idea_templates: tuple[dict[str, Any], ...]


TOPIC_PROFILES: dict[str, TopicProfile] = {
    "reasoning": TopicProfile(
        key="reasoning",
        label="LLM reasoning",
        positive=(
            "reasoning",
            "chain-of-thought",
            "chain of thought",
            "test-time compute",
            "test time compute",
            "test-time scaling",
            "verification",
            "verifier",
            "process reward",
            "reinforcement learning",
            "deepseek-r1",
            "self-evolve",
            "tree search",
            "mcts",
            "deliberation",
            "reasoning trace",
            "reflection",
        ),
        required_any=("reasoning", "reasoner", "chain-of-thought", "test-time", "verifier", "deepseek", "mcts"),
        negative=(
            "chemical",
            "liver",
            "iot",
            "education conference",
            "robotic manipulation",
            "vision-language-action",
            "vulnerability",
            "secure software",
        ),
        canonical_queries=(
            "DeepSeek-R1 reasoning reinforcement learning process reward",
            "Scaling Test-Time Compute Without Verification or RL",
            "e3 Learning to Explore Enables Extrapolation of Test-Time Compute",
            "LLM reasoning test-time scaling verifier process reward DeepSeek-R1 2025",
            "large language model reasoning self-evolve reflection tree search MCTS 2025",
            "LLM reasoning benchmark process supervision verification limitations 2025",
        ),
        stable_claims=(
            {
                "claim": "Current LLM reasoning progress is increasingly tied to test-time compute, reinforcement learning, and verification or process feedback rather than prompting alone.",
                "status": "supported",
            },
            {
                "claim": "A central open problem is when extra reasoning budget helps, when it wastes compute, and how to route budget without strong verifiers.",
                "status": "uncertain",
            },
        ),
        idea_templates=(
            {
                "title": "Uncertainty-routed reasoning budgets",
                "hypothesis": "Routing each instance among cheap CoT, self-consistency, verifier-guided search, and fallback reflection using uncertainty/process signals improves accuracy per token.",
                "mechanism": "A lightweight controller predicts whether marginal test-time compute is valuable before escalating to expensive deliberation.",
                "minimal_test": "Compare fixed-budget vs routed-budget inference on GSM-style reasoning, planning, and a small Sokoban-style task.",
                "kill_criteria": "No improvement in accuracy-token Pareto frontier over fixed self-consistency.",
                "novelty_risk": "Medium: related to test-time scaling and self-consistency; novelty depends on routing signals and verifier-availability regimes.",
                "feasibility": "High: can be evaluated with existing inference pipelines and logged uncertainty/process features.",
            },
            {
                "title": "Verifier-absence stress tests",
                "hypothesis": "Reasoning methods that rely on outcome verifiers fail differently from methods trained to self-critique process traces.",
                "mechanism": "Build paired tasks with and without reliable final checkers, then measure degradation under verifier removal.",
                "minimal_test": "Run DeepSeek-style reasoning, self-reflection, and MCTS prompting on tasks with hidden or delayed outcomes.",
                "kill_criteria": "All methods degrade similarly, or the benchmark cannot separate verifier dependence.",
                "novelty_risk": "Medium-low: many verifier papers exist, but controlled absence/quality stress tests are less standardized.",
                "feasibility": "Medium: needs careful benchmark construction but small-scale tests are cheap.",
            },
        ),
    ),
    "icl": TopicProfile(
        key="icl",
        label="LLM in-context learning",
        positive=(
            "in-context learning",
            "in context learning",
            "icl",
            "demonstration",
            "demonstrations",
            "example selection",
            "few-shot",
            "few shot",
            "prompt",
            "long-context",
            "long context",
            "retrieval",
            "meta-learning",
            "meta learning",
            "implicit dynamics",
            "task vector",
            "context compression",
        ),
        required_any=("in-context", "context learning", "demonstration", "example selection", "few-shot", "task vector"),
        negative=("protein", "liver", "iot", "vulnerability", "diffusion policy", "robotic", "chemical"),
        canonical_queries=(
            "Active Example Selection for In-Context Learning",
            "Learning without training implicit dynamics of in-context learning",
            "Unlocking the Power of LLM Uncertainty for Active In-Context Example Selection",
            "LLM in-context learning example selection demonstration retrieval 2025",
            "in-context learning mechanism implicit dynamics task vector transformer 2025",
            "long-context in-context learning context compression demonstrations 2025",
        ),
        stable_claims=(
            {
                "claim": "ICL quality is strongly shaped by example selection, ordering, task similarity, and how context induces an implicit task representation.",
                "status": "supported",
            },
            {
                "claim": "Long-context ICL creates a selection/compression problem: more examples are not automatically better unless they preserve task-relevant signal.",
                "status": "uncertain",
            },
        ),
        idea_templates=(
            {
                "title": "Task-vector stabilized demonstration selection",
                "hypothesis": "Selecting demonstrations that produce a stable internal task vector improves ICL robustness more than semantic similarity alone.",
                "mechanism": "Score candidate demos by output consistency plus representation stability under paraphrase/order perturbations.",
                "minimal_test": "Compare nearest-neighbor, diversity, uncertainty, and task-vector-stability selection on classification and reasoning ICL suites.",
                "kill_criteria": "Representation stability does not predict held-out ICL accuracy beyond simple retrieval baselines.",
                "novelty_risk": "Medium: mechanism work on ICL exists; novelty is using stability as an operational selection criterion.",
                "feasibility": "Medium: requires model activations/logprobs; feasible with open-weight LMs.",
            },
            {
                "title": "Budgeted long-context ICL compressor",
                "hypothesis": "A compressor that preserves decision-boundary examples and discards redundant demonstrations outperforms naive long-context packing.",
                "mechanism": "Cluster candidate demos by behavior, keep boundary/contradictory cases, and summarize redundant clusters into compact rules.",
                "minimal_test": "Evaluate full-context vs compressed-context ICL under equal token budgets on tasks with many candidate demonstrations.",
                "kill_criteria": "Compression loses too much task signal or does not beat retrieval-only selection.",
                "novelty_risk": "Medium: related to retrieval and prompt compression; novelty is boundary-preserving compression for ICL.",
                "feasibility": "High: can be prototyped with existing retrieval and summarization tools.",
            },
        ),
    ),
    "gui": TopicProfile(
        key="gui",
        label="LLM+GUI agents",
        positive=(
            "gui",
            "graphical user interface",
            "computer use",
            "web agent",
            "browser",
            "desktop",
            "screen",
            "osworld",
            "webarena",
            "mind2web",
            "mobileuse",
            "phone automation",
            "grounding",
            "accessibility tree",
            "multimodal gui",
            "gui agent",
        ),
        required_any=("gui", "computer use", "web agent", "osworld", "webarena", "mind2web", "screen", "phone automation"),
        negative=("chemical", "robotics survey", "materials", "protein", "liver"),
        canonical_queries=(
            "OSWorld benchmark multimodal computer use GUI agents",
            "WebArena benchmark autonomous web agents language models",
            "Mind2Web generalist web agent benchmark",
            "GUI-World video benchmark multimodal GUI understanding",
            "LLM GUI agent benchmark OSWorld WebArena Mind2Web 2025",
            "computer use agent GUI grounding accessibility tree feedback 2025",
            "multimodal GUI agents phone automation benchmark planning 2025",
        ),
        stable_claims=(
            {
                "claim": "GUI agent work is moving from web-only interaction toward multimodal desktop/mobile benchmarks with grounding, planning, and recovery challenges.",
                "status": "supported",
            },
            {
                "claim": "The main gap is reliable post-action state verification and recovery under dynamic UI changes, not only better screenshot understanding.",
                "status": "uncertain",
            },
        ),
        idea_templates=(
            {
                "title": "State-change verifier for GUI agents",
                "hypothesis": "Checking predicted UI state transitions against observed screen/accessibility-tree deltas improves recovery and reduces compounding GUI errors.",
                "mechanism": "Before acting, the agent predicts expected UI deltas; after acting, a verifier compares screen, DOM/accessibility tree, and task state to decide continue/retry/ask.",
                "minimal_test": "Add state-delta verification to a GUI benchmark subset such as Mind2Web/WebArena/OSWorld-style tasks and measure recovery after injected UI perturbations.",
                "kill_criteria": "Verifier false positives cause more retries than successful recoveries, or accessibility deltas are unavailable/unreliable.",
                "novelty_risk": "Medium: reflection and GUI grounding exist; novelty depends on explicit state-transition contracts and perturbation recovery evaluation.",
                "feasibility": "High for web/desktop tasks with observable DOM/accessibility state; medium for pure screenshots.",
            },
            {
                "title": "Hierarchical GUI affordance maps",
                "hypothesis": "Separating high-level task planning from a learned affordance map of clickable/editable UI regions improves GUI agent transfer across apps.",
                "mechanism": "Build an intermediate affordance graph from screenshot plus accessibility tree, then plan over semantic UI actions instead of raw coordinates.",
                "minimal_test": "Compare direct coordinate action prediction vs affordance-graph planning on cross-site or cross-app held-out tasks.",
                "kill_criteria": "Affordance graph construction is too brittle or adds latency without improving transfer.",
                "novelty_risk": "Medium-high: GUI grounding and accessibility-tree agents are active; novelty is reusable affordance maps plus transfer tests.",
                "feasibility": "Medium: requires environment instrumentation but can start with browser tasks.",
            },
        ),
    ),
}


def infer_topic_profile(text: str) -> TopicProfile:
    haystack = _norm(text)
    best_key = "reasoning"
    best_score = -1
    for key, profile in TOPIC_PROFILES.items():
        score = sum(2 for phrase in profile.positive if _has_phrase(haystack, phrase))
        score += sum(3 for phrase in profile.required_any if _has_phrase(haystack, phrase))
        if key == "icl" and _has_phrase(haystack, "in-context"):
            score += 8
        if key == "gui" and (_has_phrase(haystack, "gui") or _has_phrase(haystack, "computer use")):
            score += 8
        if score > best_score:
            best_key = key
            best_score = score
    return TOPIC_PROFILES[best_key]


def expand_queries(question: str, queries: list[str], *, max_queries: int = 4) -> list[str]:
    profile = infer_topic_profile(question + "\n" + "\n".join(queries))
    expanded: list[str] = []
    for query in [*profile.canonical_queries, *queries]:
        clean = " ".join(str(query).split())
        if clean and clean not in expanded:
            expanded.append(clean)
    return expanded[: max(1, int(max_queries or 4))]


def score_source(
    source: dict[str, Any],
    *,
    question: str,
    query: str = "",
) -> dict[str, Any]:
    profile = infer_topic_profile(question + "\n" + query)
    title = str(source.get("title") or "")
    notes = str(source.get("notes") or "")
    metadata = source.get("metadata") if isinstance(source.get("metadata"), dict) else {}
    abstract = str(metadata.get("abstract") or notes)
    full = _norm(" ".join([title, abstract, str(metadata.get("venue") or ""), str(metadata.get("provider") or "")]))
    title_norm = _norm(title)

    score = 0.0
    matched: list[str] = []
    negatives: list[str] = []
    for phrase in profile.positive:
        if _has_phrase(title_norm, phrase):
            score += 2.5
            matched.append(phrase)
        elif _has_phrase(full, phrase):
            score += 1.0
            matched.append(phrase)
    if profile.required_any and any(_has_phrase(full, phrase) for phrase in profile.required_any):
        score += 2.0
    else:
        score -= 2.5
    for phrase in profile.negative:
        if _has_phrase(title_norm, phrase):
            score -= 4.0
            negatives.append(phrase)
        elif _has_phrase(full, phrase):
            score -= 2.0
            negatives.append(phrase)

    provider = str(metadata.get("provider") or source.get("source_type") or "")
    if provider == "zotero" and matched:
        score += 0.8
    year = _int_or_none(metadata.get("year"))
    if year and year >= 2024:
        score += 0.8
    if metadata.get("citation_count"):
        score += min(1.0, _int_or_none(metadata.get("citation_count")) / 250)
    if metadata.get("pdf_url"):
        score += 0.4
    if any(_has_phrase(title_norm, word) for word in ("survey", "benchmark", "technical report")):
        score += 0.4

    relevance = max(0, min(5, round(score)))
    evidence_quality = _evidence_quality(metadata, source, relevance)
    role = _infer_role(title + " " + abstract, profile)
    status = "candidate" if relevance >= 2 else "rejected"
    if relevance >= 4 and role in {"benchmark", "method", "survey", "counterevidence"}:
        status = "kept"
    return {
        "profile": profile.key,
        "score": score,
        "relevance": relevance,
        "evidence_quality": evidence_quality,
        "role": role,
        "status": status,
        "matched_terms": sorted(set(matched))[:12],
        "negative_terms": sorted(set(negatives))[:8],
    }


def synthesize_ledgers(
    *,
    question: str,
    sources: list[dict[str, Any]],
    max_claims: int = 4,
    max_ideas: int = 3,
) -> dict[str, Any]:
    profile = infer_topic_profile(question)
    top_sources = [source for source in sources if source.get("status") in {"kept", "candidate"}]
    top_sources.sort(
        key=lambda source: (
            int(source.get("relevance") or 0),
            int(source.get("evidence_quality") or 0),
            source.get("updated_at") or 0,
        ),
        reverse=True,
    )
    source_titles = [str(source.get("title") or "") for source in top_sources[:6]]
    claims = []
    for template in profile.stable_claims[:max_claims]:
        claims.append(
            {
                "claim": template["claim"],
                "status": template.get("status", "uncertain"),
                "support": "; ".join(source_titles[:3]),
                "confidence": 3 if source_titles else 1,
                "metadata": {"generated_by": "local_synthesis", "profile": profile.key},
            }
        )

    ideas = []
    for template in profile.idea_templates[:max_ideas]:
        novelty = _novelty_assessment(template, top_sources)
        score = {
            "evidence": min(5, 2 + len(source_titles[:3])),
            "novelty": novelty["score"],
            "feasibility": 4 if "High" in template.get("feasibility", "") else 3,
            "project_fit": 3,
            "information_gain": 4,
        }
        ideas.append(
            {
                "title": template["title"],
                "hypothesis": template["hypothesis"],
                "mechanism": template["mechanism"],
                "minimal_test": template["minimal_test"],
                "kill_criteria": template["kill_criteria"],
                "score": score,
                "notes": (
                    f"Feasibility: {template['feasibility']}\n"
                    f"Novelty: {novelty['label']}. {template['novelty_risk']}\n"
                    f"Nearest threats: {novelty['nearest_threats'] or 'none found in current ledger'}"
                ),
                "metadata": {
                    "generated_by": "local_synthesis",
                    "profile": profile.key,
                    "nearest_threats": novelty["nearest_threats"],
                    "source_titles": source_titles[:6],
                },
            }
        )
    return {
        "profile": profile.key,
        "claims": claims,
        "ideas": ideas,
        "top_sources": source_titles[:8],
        "coverage": coverage_summary(question, sources),
    }


def select_sources_for_deep_read(
    *,
    question: str,
    sources: list[dict[str, Any]],
    max_sources: int = 3,
) -> list[dict[str, Any]]:
    infer_topic_profile(question)
    candidates: list[dict[str, Any]] = []
    for source in sources:
        if source.get("status") == "rejected":
            continue
        if source.get("depth") == "deep":
            continue
        location = _source_read_location(source)
        if not location:
            continue
        candidates.append({**source, "read_location": location})
    candidates.sort(
        key=lambda source: (
            int(source.get("relevance") or 0),
            int(source.get("evidence_quality") or 0),
            1 if _source_metadata(source).get("pdf_url") else 0,
            source.get("updated_at") or 0,
        ),
        reverse=True,
    )
    return candidates[: max(1, int(max_sources or 3))]


def threat_queries_for_idea(
    idea: dict[str, Any],
    *,
    question: str,
    max_queries: int = 3,
) -> list[str]:
    profile = infer_topic_profile(question)
    title = str(idea.get("title") or "")
    hypothesis = str(idea.get("hypothesis") or "")
    mechanism = str(idea.get("mechanism") or "")
    base = title or hypothesis or mechanism
    queries = [
        f'"{base}" related work',
        f"{base} {profile.label} benchmark baseline",
        f"{title} {mechanism} novelty threat nearest work",
    ]
    if profile.key == "gui":
        queries.append(f"{title} GUI agent grounding verification recovery accessibility tree")
    elif profile.key == "icl":
        queries.append(f"{title} in-context learning example selection task vector prompt compression")
    elif profile.key == "reasoning":
        queries.append(f"{title} LLM reasoning test-time compute verifier self-consistency")
    deduped: list[str] = []
    for query in queries:
        clean = " ".join(query.split()).strip()
        if clean and clean not in deduped:
            deduped.append(clean)
    return deduped[: max(1, int(max_queries or 3))]


def critique_idea(
    idea: dict[str, Any],
    *,
    question: str,
    sources: list[dict[str, Any]],
    threat_sources: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    profile = infer_topic_profile(question)
    threats = threat_sources or _nearest_threat_sources(idea, sources)
    threat_titles = [str(source.get("title") or "") for source in threats[:6]]
    idea_text = " ".join(
        str(idea.get(key) or "")
        for key in ("title", "hypothesis", "mechanism", "minimal_test", "kill_criteria")
    )
    overlap = max(
        (_token_overlap(_norm(idea_text), _norm(str(source.get("title") or "") + " " + str(source.get("notes") or ""))) for source in threats),
        default=0.0,
    )
    novelty = 4 if not threats else 3 if overlap < 0.22 else 2
    feasibility = _idea_feasibility_score(idea)
    falsifiability = 4 if idea.get("minimal_test") and idea.get("kill_criteria") else 2
    evidence = min(5, 2 + len([source for source in sources if source.get("status") != "rejected"][:3]))
    information_gain = 4 if falsifiability >= 4 else 3
    total = evidence + novelty + feasibility + falsifiability + information_gain
    if total >= 18 and novelty >= 3:
        status = "active"
    elif novelty <= 1 or falsifiability <= 1:
        status = "rejected"
    else:
        status = "candidate"
    critique = {
        "profile": profile.key,
        "status": status,
        "score": {
            "evidence": evidence,
            "novelty": novelty,
            "feasibility": feasibility,
            "falsifiability": falsifiability,
            "project_fit": 3,
            "information_gain": information_gain,
            "total": total,
        },
        "nearest_threats": threat_titles,
        "risks": _idea_risks(idea, threats),
        "refinement": _idea_refinement(idea, profile, threat_titles),
        "notes": _idea_critique_notes(idea, threat_titles, novelty, feasibility, falsifiability, total),
    }
    return critique


def coverage_summary(question: str, sources: list[dict[str, Any]]) -> dict[str, Any]:
    profile = infer_topic_profile(question)
    kept = [source for source in sources if source.get("status") in {"kept", "candidate"}]
    rejected = [source for source in sources if source.get("status") == "rejected"]
    providers: dict[str, int] = {}
    roles: dict[str, int] = {}
    for source in sources:
        metadata = source.get("metadata") if isinstance(source.get("metadata"), dict) else {}
        provider = str(metadata.get("provider") or source.get("source_type") or "unknown")
        providers[provider] = providers.get(provider, 0) + 1
        role = str(source.get("role") or "unknown")
        roles[role] = roles.get(role, 0) + 1
    return {
        "profile": profile.key,
        "total": len(sources),
        "kept_or_candidate": len(kept),
        "rejected": len(rejected),
        "precision_proxy": round(len(kept) / len(sources), 3) if sources else 0,
        "providers": providers,
        "roles": roles,
        "top_rejected": [source.get("title") for source in rejected[:5]],
    }


def _infer_role(text: str, profile: TopicProfile) -> str:
    value = _norm(text)
    if any(term in value for term in ("benchmark", "dataset", "evaluation", "osworld", "webarena", "mind2web")):
        return "benchmark"
    if any(term in value for term in ("survey", "review", "frontiers", "progress and prospects")):
        return "survey"
    if any(term in value for term in ("limitation", "stress test", "failure", "without verification", "break easily")):
        return "counterevidence"
    if any(term in value for term in ("method", "framework", "algorithm", "training", "selection", "verifier", "grounding")):
        return "method"
    return "method" if profile.key in {"reasoning", "icl", "gui"} else "source"


def _evidence_quality(metadata: dict[str, Any], source: dict[str, Any], relevance: int) -> int:
    quality = 1 if relevance else 0
    if source.get("depth") in {"abstract", "skim", "deep"}:
        quality += 1
    if metadata.get("doi") or metadata.get("provider") in {"arxiv", "zotero"}:
        quality += 1
    if metadata.get("pdf_url"):
        quality += 1
    citation_count = _int_or_none(metadata.get("citation_count"))
    if citation_count and citation_count > 50:
        quality += 1
    return max(0, min(5, quality))


def _novelty_assessment(template: dict[str, Any], sources: list[dict[str, Any]]) -> dict[str, Any]:
    idea_text = _norm(" ".join(str(template.get(key) or "") for key in ("title", "hypothesis", "mechanism")))
    threats: list[str] = []
    for source in sources:
        title = str(source.get("title") or "")
        notes = str(source.get("notes") or "")
        overlap = _token_overlap(idea_text, _norm(title + " " + notes))
        if overlap >= 0.18:
            threats.append(title)
    if len(threats) >= 3:
        return {"label": "incremental unless differentiated from close threats", "score": 2, "nearest_threats": threats[:4]}
    if threats:
        return {"label": "plausibly novel but has close adjacent work", "score": 3, "nearest_threats": threats[:4]}
    return {"label": "underexplored in current ledger; needs broader threat search", "score": 4, "nearest_threats": []}


def _nearest_threat_sources(idea: dict[str, Any], sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    idea_text = _norm(
        " ".join(str(idea.get(key) or "") for key in ("title", "hypothesis", "mechanism", "minimal_test"))
    )
    scored: list[tuple[float, dict[str, Any]]] = []
    for source in sources:
        if source.get("status") == "rejected":
            continue
        text = _norm(str(source.get("title") or "") + " " + str(source.get("notes") or ""))
        overlap = _token_overlap(idea_text, text)
        if overlap > 0.08:
            scored.append((overlap, source))
    scored.sort(key=lambda item: item[0], reverse=True)
    return [source for _score, source in scored[:8]]


def _source_read_location(source: dict[str, Any]) -> str:
    metadata = _source_metadata(source)
    for key in ("local_attachment_path", "pdf_url", "url", "open_access_pdf"):
        value = metadata.get(key)
        if value:
            return str(value)
    url = str(source.get("url") or "")
    if url.startswith(("http://", "https://", "/", "~")):
        return url
    return ""


def _source_metadata(source: dict[str, Any]) -> dict[str, Any]:
    metadata = source.get("metadata")
    return metadata if isinstance(metadata, dict) else {}


def _idea_feasibility_score(idea: dict[str, Any]) -> int:
    text = _norm(
        " ".join(str(idea.get(key) or "") for key in ("minimal_test", "mechanism", "notes"))
    )
    score = 2
    if any(term in text for term in ("compare", "evaluate", "benchmark", "ablation", "small-scale", "subset")):
        score += 1
    if any(term in text for term in ("existing", "open-weight", "browser", "web", "logged", "pipeline")):
        score += 1
    if any(term in text for term in ("requires", "new benchmark", "instrumentation", "human", "large-scale")):
        score -= 1
    return max(1, min(5, score))


def _idea_risks(idea: dict[str, Any], threats: list[dict[str, Any]]) -> list[str]:
    risks: list[str] = []
    if threats:
        risks.append("Close related work may already cover the core mechanism; differentiate on setting, signal, or evaluation.")
    if not str(idea.get("minimal_test") or "").strip():
        risks.append("Minimal test is underspecified.")
    if not str(idea.get("kill_criteria") or "").strip():
        risks.append("Kill criteria are missing or too soft.")
    if "improve" in _norm(str(idea.get("hypothesis") or "")) and "per token" not in _norm(str(idea.get("hypothesis") or "")):
        risks.append("Efficiency or cost metric may be underspecified.")
    return risks or ["No obvious blocking risk from current ledger; still needs deeper source reading."]


def _idea_refinement(idea: dict[str, Any], profile: TopicProfile, threats: list[str]) -> dict[str, str]:
    title = str(idea.get("title") or "")
    hypothesis = str(idea.get("hypothesis") or "")
    minimal_test = str(idea.get("minimal_test") or "")
    threat_clause = " against nearest-threat baselines" if threats else " against strong baselines"
    if profile.key == "gui":
        metric = "success rate, recovery rate after UI perturbation, and extra action/token cost"
    elif profile.key == "icl":
        metric = "accuracy under order/paraphrase perturbations and token-normalized performance"
    else:
        metric = "accuracy-token Pareto frontier, verifier dependence, and failure-mode coverage"
    return {
        "hypothesis": hypothesis,
        "minimal_test": f"{minimal_test} Measure {metric}{threat_clause}.",
        "differentiator": f"Make `{title}` explicit about its observable signal, evaluation regime, and failure cases.",
    }


def _idea_critique_notes(
    idea: dict[str, Any],
    threats: list[str],
    novelty: int,
    feasibility: int,
    falsifiability: int,
    total: int,
) -> str:
    lines = [
        "Idea critique:",
        f"- Novelty score: {novelty}/5",
        f"- Feasibility score: {feasibility}/5",
        f"- Falsifiability score: {falsifiability}/5",
        f"- Total score: {total}/25",
    ]
    if threats:
        lines.append("- Nearest threats: " + "; ".join(threats[:5]))
    else:
        lines.append("- Nearest threats: none found in current ledger; run broader threat search before claiming novelty.")
    return "\n".join(lines)


def _token_overlap(left: str, right: str) -> float:
    left_tokens = {token for token in re.findall(r"[a-z0-9]+", left) if len(token) > 2}
    right_tokens = {token for token in re.findall(r"[a-z0-9]+", right) if len(token) > 2}
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / len(left_tokens)


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").casefold()


def _has_phrase(text: str, phrase: str) -> bool:
    phrase = _norm(phrase)
    if not phrase:
        return False
    if re.fullmatch(r"[a-z0-9]{1,4}", phrase):
        return re.search(rf"(?<![a-z0-9]){re.escape(phrase)}(?![a-z0-9])", text) is not None
    return phrase in text


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
