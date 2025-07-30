# BGG + Finna Devcontainer

This devcontainer provides a secure, isolated development environment for the BGG + Finna Board Game Explorer project, optimized for use with Claude Code.

## Features

- **Python 3.11** with uv package manager
- **Pre-installed dependencies** from pyproject.toml
- **Network security** with firewall restrictions
- **VS Code extensions** for Python development
- **Port forwarding** for Streamlit dashboard (8501)
- **Data persistence** through Docker volumes

## Security

The devcontainer implements network restrictions following Claude Code best practices:

### Allowed Domains
- `api.finna.fi` - Finna API for Finnish library data
- `boardgamegeek.com` - BoardGameGeek API 
- `github.com` - Git operations and repository access
- `pypi.org` - Python package management
- VS Code infrastructure domains

### Blocked
- All other outbound network connections (default deny)

## Files

- **`devcontainer.json`** - Main configuration with VS Code settings and extensions
- **`Dockerfile`** - Container image with Python environment and tools
- **`init-firewall.sh`** - Network security setup (runs on container start)
- **`post-create.sh`** - Development environment setup (runs after creation)

## Usage

1. Open repository in VS Code
2. Click "Reopen in Container" when prompted
3. Container builds automatically with all dependencies
4. Start developing with full isolation and security

## Troubleshooting

### Network Issues
If you encounter network connectivity problems:
1. Check that required domains are accessible
2. Review firewall logs: `dmesg | grep DROPPED`
3. Ensure Docker has proper network access

### Permission Issues
If scripts fail to run:
```bash
chmod +x .devcontainer/*.sh
```

### Dependencies
If packages fail to install:
```bash
uv sync --group test
```

## Claude Code Integration

This devcontainer is specifically configured for Claude Code development:
- Secure network restrictions prevent unauthorized access
- All necessary APIs are whitelisted
- Development tools are pre-configured
- Data persistence ensures work is not lost between sessions