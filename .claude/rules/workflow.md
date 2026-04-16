# Workflow Orchestration

## 1. Plan Mode Default
- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, STOP and re-plan — don't keep pushing
- Write detailed specs in tasks/todo.md upfront

## 2. Subagent Strategy
- Use subagents to keep main context window clean
- Offload research, exploration, parallel analysis to subagents
- One task per subagent for focused execution

## 3. Self-Improvement Loop
- After ANY correction: update tasks/lessons.md with the pattern
- Format: ALWAYS/NEVER + concrete rule + why
- Review lessons at session start

## 4. Verification Before Done
- Never mark a task complete without proving it works
- Run tests, check logs, demonstrate correctness
- For API integrations: test with real endpoints before marking done
- Ask: "Would a staff engineer approve this?"

## 5. Demand Elegance (Balanced)
- For non-trivial changes: "is there a more elegant way?"
- Skip for simple, obvious fixes — don't over-engineer

## 6. Autonomous Bug Fixing
- When given a bug report: just fix it, don't ask for hand-holding
- Point at logs, errors, failing tests — then resolve them

## Task Management
1. **Plan First**: Write plan to tasks/todo.md with checkable items
2. **Verify Plan**: Check in before starting implementation
3. **Track Progress**: Mark items complete as you go
4. **Explain Changes**: High-level summary at each step
5. **Document Results**: Add review section to tasks/todo.md
6. **Capture Lessons**: Update tasks/lessons.md after corrections

## Core Principles
- **Simplicity First**: Minimal code that solves the problem.
- **No Laziness**: Root causes. No temporary fixes. Senior developer standards.
- **Minimal Impact**: Touch only what's necessary. Avoid introducing bugs.
- **Prove It Works**: Tests pass, logs clean, behavior verified.
