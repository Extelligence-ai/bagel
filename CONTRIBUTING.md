# How to Contribute to Bagel

## Found a Bug?

The Bagel project uses GitHub as its bug tracker. To report a bug, sign in to your GitHub account, navigate to [GitHub Issues](https://github.com/shouhengyi/bagel/issues), and click **New issue**. Before creating a new bug entry, we recommend searching existing issues first to avoid duplicates.

## Have a Patch That Fixes a Bug or Improves Bagel?

First, create a GitHub issue as described above. Then, create a new branch using the following naming convention: "category/short-description." For example:

- `fix/my-bug-fix` for bug fixes
- `feature/new-feature` for adding new features
- `doc/add-readme` for documentation improvements
- `issue/publish-pypi` for all other changes

Please use **kebab-case** for "short-description." Refer to the [kebab-case guide](https://developer.mozilla.org/en-US/docs/Glossary/Kebab_case) if you're unfamiliar with it.

Once your branch is ready, file a Pull Request (PR) against the `main` branch. In the PR description, please add text like "Closes #10" to automatically link and close the associated issue once the PR is merged.

## Working in Stacked PRs? Land Them via a Tip PR

For large arcs we review work as a stack of small PRs (each based on the previous branch), then **land the whole stack with a single "landing PR" from the stack's tip to `main`**, which the reviewer approves as the final gate. Do not merge stacked PRs individually into `main`.

Why: GitHub's branch protection interacts badly with stacks — retargeting a PR to `main` dismisses its approvals, and deleting a base branch closes child PRs un-reopenably. The landing PR keeps the audit trail honest instead: each constituent PR carries its own review, and the landing PR (which links them all) gets one final approval of the integrated whole.

Mechanics: verify the tip merges clean (`git merge-tree --write-tree origin/main <tip>`), open the landing PR listing every constituent PR, and after it merges, close the constituents with a comment pointing at the landing PR. Never delete branches mid-landing; sweep them afterwards.

## Releases

- Versions follow [semver](https://semver.org). The release checklist: bump `version` in `pyproject.toml`, tag `vX.Y.Z`, publish the GitHub release (breakfast-themed names encouraged).
- **Big release waves ship as prereleases first** (`vX.Y.0-beta.1`, marked pre-release on GitHub) with written graduation criteria — a beta without an exit plan rots. `latest` Docker tags and the *Latest* release pointer stay on stable until GA.
- **Individual risky features are labeled beta/experimental at the feature level** (README, runbook, and tool descriptions) rather than holding back the whole release; the label states what would graduate it. Never retroactively mark a shipped release as pre-release.

## Developing Bagel

To install the development PyPI dependencies, run:

```sh
uv sync --dev
```

### Pre-commit Hooks

Before pushing any commits, run the hooks in [.pre-commit-config.yaml](.pre-commit-config.yaml). They enforce linting and formatting. See [quality contracts and CI rollout](doc/runbooks/quality.md) for test dependencies, coverage, type checks, and required repository settings.

To set up the pre-commit hooks for the first time, run these commands from the repository root:

```sh
uv sync --dev  # install the pre-commit PyPI package
uv run pre-commit install  # install the pre-commit hooks
```

After this initial setup, pre-commit hooks will automatically run each time you commit changes.

### Linting

We use [`ruff`](https://docs.astral.sh/ruff/) for linting Python code. Run it with:

```sh
uv run ruff check ./
uv run ruff format --check
uv sync --locked --group quality
uv run --no-sync mypy
```

For Dockerfiles, we use [hadolint](https://github.com/hadolint/hadolint). Run it with:

```sh
hadolint docker/Dockerfile.*
```
