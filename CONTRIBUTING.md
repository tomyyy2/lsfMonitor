# Contributing

## Development Gate (Mandatory)

For this project, **every commit/PR must pass CI/CD gates**:

1. Code quality check (syntax/import/lint in later phase)
2. Build verification (core entrypoints)
3. Automated tests (pytest)
4. Deployment verification (at least dry-run on main branch)

If any gate fails, the change is not mergeable.

## Basic Flow

1. Create a feature branch
2. Commit small and focused changes
3. Open PR
4. Wait for CI pipeline to pass
5. Request review and merge

## Testing Notes

- Add or update tests for every bugfix/feature.
- Keep tests deterministic and runnable in CI.
- GUI features should include at least smoke coverage and manual test notes.
