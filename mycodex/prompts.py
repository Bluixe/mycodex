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
