from __future__ import annotations

from datetime import datetime


def daily_summary_prompt(project_name: str) -> str:
    today = datetime.now().strftime("%Y-%m-%d")
    return f"""请为 Codex project `{project_name}` 生成今天（{today}）的每日进度总结。

请在项目目录内只读检查必要信息，例如 git commits、git status、最近修改文件、测试/实验日志、README 或任务文件。不要修改文件。

输出请包含：
1. 今天完成了什么
2. 当前工作区状态和潜在风险
3. 最近 commits 或关键变更摘要
4. 实验/测试/CI 进展，如果能从本地信息判断
5. 明天或下一步最值得做的 3-5 项
6. 需要用户决策或补充信息的事项
"""


def experiment_control_prompt(
    *,
    project_name: str,
    experiment_name: str,
    experiment_id: int,
    objective: str,
    max_iterations: int | None,
    remote_ref: str,
) -> str:
    limit = str(max_iterations) if max_iterations is not None else "not specified"
    remote = remote_ref or "not configured yet"
    return f"""请作为 Codex project `{project_name}` 的自我迭代实验控制 session，管理 mycodex experiment `{experiment_name}` (id={experiment_id})。

实验目标：
{objective or "(尚未填写；请先要求用户补充具体目标、指标、运行命令和远程执行环境。)"}

迭代上限：{limit}
远程执行引用：{remote}

请遵守：
1. 先阅读 `AGENTS.md` 和 `docs/codex-memory.md`，只基于可验证信息制定计划。
2. 在用户补充远程服务器、命令、数据路径、预算、指标和停止条件前，不要启动昂贵训练或远程任务。
3. 每一轮迭代都要明确：hypothesis、baseline、intervention、command、metrics、artifacts、risk。
4. 不要直接写 mycodex SQLite，也不要从这个 session 自行调用 `mycodex experiments event`；由外层 my-codex 控制平面统一写事件，避免锁冲突。
5. 你需要在回答中给出外层控制平面可执行的结构化建议：patch 摘要、需要提交/推送的文件、远程启动命令、W&B run 识别方式、关键指标、停止条件、异常处理规则。
6. 如果你判断下一步是低风险的 read-only preflight、diagnostic patch、monitor-only 或 5-10 step smoke training，并且 patch scope、命令、预算、W&B 身份、指标和 hard stop 条件都完整，请明确标记 `auto_execute: true`，让外层 my-codex 不再等待用户确认，直接改代码、提交、推送、远程启动和监控。
7. 如果还不能实际执行实验，请输出 `auto_execute: false`、阻塞原因、外部执行计划和缺失参数清单。

输出请包含：
1. 当前实验是否具备启动条件
2. 建议的迭代状态机
3. 第一轮实验的最小可执行方案
4. 需要 mycodex 监控的事件和指标
5. 需要用户补充的远程执行细节
6. 结构化执行契约，必须放在单独的 JSON code block 中，格式如下：

```json
{{
  "auto_execute": true,
  "execution_class": "read-only-preflight | diagnostic-patch | smoke-train | full-train | monitor-only | blocked",
  "reason": "一句话说明为什么应该自动执行或为什么阻塞",
  "patch": {{
    "required": true,
    "files": ["path/to/file.py"],
    "summary": "改动摘要",
    "commit_message": "提交信息",
    "unified_diff": "如果 required=true，给出可由 git apply 应用的完整 unified diff；如果不需要改代码则为空字符串"
  }},
  "local_checks": ["pytest ..."],
  "remote": {{
    "command": "远程执行命令",
    "budget": "例如 5-10 steps / wall time / GPU",
    "resume_from": "checkpoint path 或 none"
  }},
  "wandb": {{
    "entity": "...",
    "project": "...",
    "run_id": "...",
    "run_name": "..."
  }},
  "metrics": ["metric/name"],
  "hard_stop": ["非有限值、crash、指标阈值等"],
  "soft_review": ["需要复核但不一定停止的条件"]
}}
```
"""


def research_control_prompt(
    *,
    project_name: str,
    research_name: str,
    research_id: int,
    question: str,
    max_iterations: int | None,
    ledger_context: str = "",
) -> str:
    limit = str(max_iterations) if max_iterations is not None else "not specified"
    context_block = ledger_context.strip() or "(当前没有可用 ledger snapshot；请先制定低风险 search/triage 任务。)"
    return f"""请作为 Codex project `{project_name}` 的自动研究控制 session，管理 mycodex research `{research_name}` (id={research_id})。

研究问题：
{question or "(尚未填写；请先将用户目标改写成明确 research question，并列出需要补充的范围约束。)"}

迭代上限：{limit}

外层 mycodex 已注入当前 research ledger snapshot。它是本轮决策的主要上下文：

```json
{context_block}
```

请遵守：
1. 先阅读 `AGENTS.md`、`docs/codex-memory.md` 和可用 research skills，只基于可验证信息推进。
2. 你的目标不是输出一篇长报告，而是审查并维护 source ledger、claim ledger、idea ledger 和 next-action queue。
3. 你需要作为更深的研究决策层：检查 coverage、grounding、nearest threats、idea novelty、可行性、kill criteria 和下一轮信息增益。
4. 低风险动作可以建议自动继续，例如公开来源检索、deep_read_top、paper triage、threat_search、idea_critique、claim verification、synthesis、实验设计草案。
5. 对 `search`、`deep_read_top`、`threat_search`、`idea_critique` 这类动作，你只需要决定它们是否值得做；外层 mycodex local worker 会执行并写回 ledger。
6. 需要用户决策的动作必须停下：选择研究方向、写代码、启动实验、使用凭证/付费/私有来源、或做大段正式写作。
7. 不要直接写 mycodex SQLite，也不要从这个 session 自行调用 `mycodex research event`；由外层 my-codex 控制平面统一写事件和 ledger。
8. 重要事实必须能追溯到 source；如果只是推断，请标记为 inference。
9. 每轮都要给出紧凑 research packet，并在最后输出一个单独 JSON code block，供外层控制平面解析。

输出请包含：
1. 当前研究状态和阶段
2. 本轮 evidence update
3. claim/idea 的变化
4. idea tournament 或下一步筛选逻辑：必须明确哪些 idea 继续、降级、淘汰，以及 nearest threats
5. 下一步是否可以 auto_continue
6. 结构化 research contract，必须放在单独的 JSON code block 中，格式如下：

```json
{{
  "research_question": "当前研究问题",
  "phase": "scan | triage | synthesis | idea_tournament | experiment_bridge | blocked",
  "auto_continue": true,
  "next_action": {{
    "type": "search | triage | deep_read | deep_read_top | threat_search | idea_critique | synthesize | rank_ideas | design_experiment | ask_user",
    "reason": "为什么这是下一步",
    "queries": ["需要继续搜索的 query"],
    "sources_to_read": ["需要精读的 source title 或 url"],
    "source_ids": [1],
    "idea_ids": [1],
    "stop_condition": "什么时候停止该步骤"
  }},
  "source_ledger_delta": [
    {{
      "title": "source title",
      "url": "source url",
      "source_type": "paper | code | benchmark | docs | blog | other",
      "role": "baseline | method | benchmark | survey | tool | counterevidence",
      "depth": "title | abstract | skim | deep",
      "keep": true,
      "relevance": 0,
      "evidence_quality": 0,
      "notes": "为什么保留或拒绝"
    }}
  ],
  "claim_ledger_delta": [
    {{
      "claim": "具体 claim",
      "support": "source title/url 或 inference",
      "status": "supported | contradicted | uncertain",
      "confidence": 0,
      "notes": "证据限制"
    }}
  ],
  "idea_ledger_delta": [
    {{
      "idea": "idea short name",
      "hypothesis": "可证伪假设",
      "mechanism": "为什么可能有效",
      "minimal_test": "最小测试",
      "score": {{
        "evidence": 0,
        "novelty": 0,
        "feasibility": 0,
        "project_fit": 0,
        "information_gain": 0
      }},
      "kill_criteria": ["什么结果会杀死这个 idea"],
      "status": "candidate"
    }}
  ],
  "ask_user_if": ["需要用户决策的条件"]
}}
```
"""
