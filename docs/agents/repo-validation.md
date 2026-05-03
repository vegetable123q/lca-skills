---
title: skills Validation Guide
docType: guide
scope: repo
status: active
authoritative: true
owner: skills
language: en
whenToUse:
  - when validating changed skills, wrappers, packaging rules, or documentation governance
  - when selecting proof for a skills repository PR
whenToUpdate:
  - when skill validation commands change
  - when wrapper or packaging proof expectations change
  - when docpact governance rules or CI behavior change
checkPaths:
  - AGENTS.md
  - .docpact/config.yaml
  - .github/workflows/ai-doc-lint.yml
  - scripts/validate-skills.mjs
  - test/**
  - "*/SKILL.md"
  - "*/agents/openai.yaml"
lastReviewedAt: 2026-05-03
lastReviewedCommit: 5004ca88f8c2b7177a90fe696eafaf76d0e813cf
related:
  - AGENTS.md
  - .docpact/config.yaml
  - docs/agents/repo-architecture.md
---

# skills Validation Guide

The canonical local validation command is:

```bash
node scripts/validate-skills.mjs
```

You may pass one or more skill directories to validate only the touched skill packages.

## Required Validation Shape

- Skill instruction changes require validating the touched skill package.
- Wrapper contract changes require checking the paired `agents/openai.yaml` and `SKILL.md` together.
- Validation-script or test changes require running the full `node scripts/validate-skills.mjs` command when feasible.
- Documentation-governance changes require docpact validation.

## Docpact Validation

Run these commands for governance changes:

```bash
docpact validate-config --root . --strict
docpact lint --root . --base origin/main --head HEAD --mode enforce
```

The repository PR workflow runs the same docpact config validation and PR-shaped lint gate.
