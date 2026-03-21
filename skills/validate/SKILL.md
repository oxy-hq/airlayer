---
name: validate
description: Validate .view.yml semantic layer files using o3. Use when the user creates or modifies view files and wants to check for errors.
allowed-tools:
  - Bash
  - Read
  - Glob
argument-hint: "[--path <directory>]"
---

# o3 Validate — Check Semantic Layer Files

Validate `.view.yml` files for correctness.

## Prerequisites

```bash
which o3 || cargo install --git https://github.com/oxy-hq/o3
```

## Find and validate

1. Locate the base directory containing `views/`:

```bash
find . -name "*.view.yml" -not -path "*/node_modules/*" -not -path "*/cube/*" 2>/dev/null | head -10
```

2. Run validation:

```bash
o3 validate --path <base_dir>
```

$ARGUMENTS

## What it checks

- All views have either `table` or `sql` (not both, not neither)
- No duplicate view names
- Entity key references point to existing dimensions
- Dimension/measure types are valid
- Cross-entity references (`{{entity.field}}`) resolve correctly
- Measure-to-measure references (`{{view.measure}}`) resolve correctly

## On error

Show the user the validation error and the relevant section of the `.view.yml` file. Suggest a fix.
