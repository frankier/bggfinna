#!/bin/bash

# post-create.sh - Setup script run after devcontainer creation
# Installs project dependencies and configures the development environment

set -e

echo "🚀 Setting up BGG + Finna development environment..."

# Ensure we're in the right directory
cd /workspaces/bggfinna

# Install/update project dependencies
echo "📦 Installing project dependencies with uv..."
uv sync --group test

# Create necessary directories
echo "📁 Creating project directories..."
mkdir -p data/test data/smoke logs

# Set up git configuration (if not already configured)
if [ -z "$(git config --global user.name)" ]; then
    echo "⚙️  Note: Git user not configured. You may want to run:"
    echo "   git config --global user.name 'Your Name'"
    echo "   git config --global user.email 'your.email@example.com'"
fi

# Set up Claude Code permissions if settings exist
if [ -f ".claude/settings.local.json" ]; then
    echo "🤖 Claude Code settings found - devcontainer ready for Claude Code!"
else
    echo "⚠️  No Claude Code settings found. You may need to configure .claude/settings.local.json"
fi

# Run a quick smoke test to verify everything works
echo "🧪 Running quick smoke test..."
if uv run python -c "import requests, duckdb, pandas, streamlit; print('✅ All dependencies imported successfully')"; then
    echo "✅ Development environment ready!"
else
    echo "❌ Some dependencies failed to import. Please check the setup."
    exit 1
fi

echo ""
echo "🎉 BGG + Finna devcontainer setup complete!"
echo ""
echo "📖 Quick start:"
echo "   • Run pipeline: uv run python run_pipeline.py"
echo "   • Run tests: uv run pytest"
echo "   • Start dashboard: uv run streamlit run dashboard.py"
echo "   • Run smoke test: uv run python test_smoke.py"
echo ""
echo "🔒 Security: Network access is restricted to necessary APIs only"