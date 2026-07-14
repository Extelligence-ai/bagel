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

## Developing Bagel

To install the development PyPI dependencies, run:

```sh
uv sync --dev
```

### Pre-commit Hooks

Before pushing any commits, we require you to run the pre-commit hooks defined in [.pre-commit-configs.yaml](https://www.google.com/search?q=.pre-commit-configs.yaml). If you're new to pre-commit hooks, please refer to the [pre-commit documentation](https://pre-commit.com/). The hooks we enforce primarily focus on linting and formatting.

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
```

For Dockerfiles, we use [hadolint](https://github.com/hadolint/hadolint). Run it with:

```sh
hadolint docker/Dockerfile.*
```
