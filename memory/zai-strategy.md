# Z.ai Strategy — TW Stock Hunter (Updated 2026-05-15)

## New Workflow: Z.ai Codes, Crabby Reviews

**Session URL:** `https://chat.z.ai/c/949ec56b-f2fa-438a-ae1b-ca1e0cdc9a56` (read from `.zai-chat-url`)
**Mode:** Agent mode — Z.ai has full GitHub repo access, can commit/push/pull

### How It Works
1. **Me:** Send task prompt to Z.ai describing what Phase N needs (from .improvement-phase.json)
2. **Z.ai:** Codes directly on the GitHub repo, commits changes, pushes to origin/main
3. **Me:** Pull latest locally, review `git diff`, check for bugs/edge cases/Taiwan-market correctness
4. **Feedback loop:** If fixes needed, send feedback → Z.ai applies corrections directly on repo
5. **Verify:** Run local tests, update daily log, increment phase tracker

### Key Change From Old Pattern
- ~~Crabby implements code locally, sends diff for review~~ ❌
- ✅ **Z.ai codes on the repo directly via agent mode**
- I am now the **reviewer**, not the implementer
- No more async polling — Z.ai agent handles its own execution

### What To Tell Z.ai (Task Prompt Template)
```
TW Stock Hunter — Phase N: [Title]
Read .improvement-phase.json for context on completed phases and current goal.

Current task: [Describe what needs to be done, referencing specific files/functions]
Files involved: core/xxx.py (describe current state if relevant)
Requirements:
1. [Specific change 1]
2. [Specific change 2]
Constraints: [Taiwan market specifics, coding standards from prior phases]
Test command: python3 core/fetch_data.py --date YYYY-MM-DD --dry-run

After implementing, commit with message "Phase N: [brief description]" and push to main.
```

### Cron Jobs Updated (2026-05-15)
- Night iteration `3f1b0e69` — updated payload
- Day iteration `64407c3e` — updated payload
- Weekend iteration `85ee3874` — updated payload

### Notes
- Z.ai agent sees entire repo history, so context about prior phases is preserved naturally
- Still need local dry-run testing (Z.ai can't run Python on my machine)
- Memory logging and phase tracking still done by me locally