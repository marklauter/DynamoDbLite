# Tech Debt

Track technical debt as individual markdown files in this folder (docs/tech-debt).

## For AI Agents

When adding a tech debt item, create a new file in this folder following the format below. Use the next available number prefix. Do not modify existing items unless explicitly asked.

## File Naming

`NNN-short-slug.md` (e.g., `001-no-connection-pooling.md`)

## File Format

```markdown
# Short descriptive title

- **Area:** Component or subsystem name
- **Type:** Bug | Performance | Observability | Validation | Cleanup | Feature Gap | Safety | API Fidelity | Testing
- **Priority:** High | Medium | Low
- **Status:** Open | In Progress | Resolved | Won't Fix

## Problem
What is wrong and why it matters.

## Suggested Fix
How to address it.

## Code References
Key locations in the codebase related to this debt.
- `path/to/file.cs:line` — brief description of what's here
- `path/to/other.cs:line` — brief description of what's here

## Notes
Optional context, constraints, or related items.
```

## Type Values

- **Bug** — incorrect behavior vs. real DynamoDB
- **Performance** — allocation, hot-path, or scaling concern
- **Observability** — swallowed exceptions, missing logging
- **Validation** — missing or insufficient input checking
- **Cleanup** — dead code, structural refactor, naming
- **Feature Gap** — unimplemented DynamoDB capability
- **Safety** — error handling, null guards, edge-case hardening
- **API Fidelity** — response shape or error code mismatch vs. AWS
- **Testing** — missing coverage or test quality

## Status Values

- **Open** — identified, not yet started
- **In Progress** — actively being worked on
- **Resolved** — fixed (keep the file for history)
- **Won't Fix** — intentional decision not to address
