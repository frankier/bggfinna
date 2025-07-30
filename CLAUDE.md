## Purpose

The purpose of this repository is to grab all the boad games available in a particular region of Finnish libraries, given as input. Information from Board Game Geek (BGG) is then used to obtain ranks. Then we can see the top ranked games we can get from the library, and also filter by other stuff like "family" and "co-op"

This is done using a pipeline which ends up as a DuckDB database.

A Streamlit dashboard allows the user to browse the database.

## Coding style

 * Be succinct!
 * PEP-8 where possible.
 * Don't swallow unknown exceptions or write generic error handlers, just let the default error bubble up.
 * Don't make programs output too much and be too chatty.
 * Try to make code speak for itself, but short docstrings and occasional comments are welcome.

## Background

 * Finna
   * Finna API docs entrypoint: https://www.kiwi.fi/spaces/Finna/pages/53839221/Finna+API+in+English
   * Find further information through web search and looking through those pages
 * BGG
   * There is an API documented at https://boardgamegeek.com/wiki/page/BGG_XML_API2

## Current Configuration

- Claude Code settings are configured in `.claude/settings.local.json`
- Repository is configured for PR-based workflow with Claude Code
- Current permissions allow git and GitHub CLI commands
- GitHub repository: https://github.com/frankier/bggfinna

## Pull Request Workflow

This repository uses a PR-based development workflow:

1. **Before Starting**: Always pull the latest main branch before starting new work
2. **Making Changes**: When requesting changes from Claude Code, work will be done on feature branches (prefixed with `claude-`)
3. **Automatic PRs**: Claude Code will automatically create pull requests for changes
4. **CI Testing**: GitHub Actions runs three test jobs on every PR:
   - **Smoke Test**: Complete pipeline with 1 record (`BGGFINNA_TEST=2`)
   - **Unit Tests**: Existing pytest test suite
   - **Integration Test**: Full pipeline with limited data (`BGGFINNA_TEST=1`)
5. **Code Review**: Comment `@claudereview` on PRs to trigger automated Claude code review
6. **Review & Merge**: Review PRs in GitHub before merging to main branch

## Testing

The project includes multiple test modes controlled by the `BGGFINNA_TEST` environment variable:

- **Production Mode** (unset): Full pipeline, outputs to `data/`
- **Test Mode** (`BGGFINNA_TEST=1`): 10 records, outputs to `data/test/`
- **Smoke Test Mode** (`BGGFINNA_TEST=2`): 1 record, outputs to `data/smoke/`

Run the smoke test locally with:
```bash
uv run python test_smoke.py
```

## Development Setup

### Option 1: Devcontainer (Recommended for Claude Code)

The project includes a devcontainer configuration for isolated, secure development with Claude Code:

**Prerequisites:**
- VS Code with Remote - Containers extension
- Docker Desktop

**Setup:**
1. Open the repository in VS Code
2. Click "Reopen in Container" when prompted (or use Command Palette > "Remote-Containers: Reopen in Container")
3. VS Code will build the container and set up the environment automatically
4. The container includes:
   - Python 3.11 with uv package manager
   - All project dependencies pre-installed
   - Network firewall restricting access to necessary APIs only
   - VS Code extensions for Python development
   - Streamlit port forwarding (8501)

**Security Features:**
- Isolated container environment
- Network firewall allowing only necessary APIs:
  - `api.finna.fi` (Finna API)
  - `boardgamegeek.com` (BGG API)
  - `github.com` (Git operations)
  - Python package repositories
- Data persistence through Docker volumes

### Option 2: Local Development

Tools and libraries in use include:
 
 * Python (run with `. "$HOME/.cargo/env" && uv run python`)
 * uv
 * requests
 * DuckDB
 * Streamlit

You might need to add `$HOME/.cargo/bin` to your `$PATH` to run uv.

Streamlit does not show errors when with the `streamlit` command so always try running the script with `python` first.

## Usage

Refer to @README.md
