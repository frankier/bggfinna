#!/bin/bash

# init-firewall.sh - Initialize network security rules for Claude Code devcontainer
# This script sets up a restrictive firewall that only allows necessary outbound connections

set -e

echo "🔒 Initializing devcontainer firewall for BGG + Finna project..."

# Check if running as root (required for iptables)
if [ "$EUID" -ne 0 ]; then
    echo "⚠️  Warning: Firewall setup requires root privileges. Some network restrictions may not be applied."
    exit 0
fi

# Clear existing rules
iptables -F OUTPUT
iptables -F INPUT

# Default policies
iptables -P INPUT ACCEPT
iptables -P FORWARD DROP
iptables -P OUTPUT DROP

# Allow loopback traffic (required for local development)
iptables -A INPUT -i lo -j ACCEPT
iptables -A OUTPUT -o lo -j ACCEPT

# Allow established and related connections
iptables -A OUTPUT -m state --state ESTABLISHED,RELATED -j ACCEPT

# Allow DNS resolution
iptables -A OUTPUT -p udp --dport 53 -j ACCEPT
iptables -A OUTPUT -p tcp --dport 53 -j ACCEPT

# Allow necessary API endpoints for the project
echo "📡 Configuring API access rules..."

# Finna API (Finnish libraries)
iptables -A OUTPUT -p tcp -d api.finna.fi --dport 443 -j ACCEPT
iptables -A OUTPUT -p tcp -d api.finna.fi --dport 80 -j ACCEPT

# BoardGameGeek API
iptables -A OUTPUT -p tcp -d boardgamegeek.com --dport 443 -j ACCEPT
iptables -A OUTPUT -p tcp -d boardgamegeek.com --dport 80 -j ACCEPT
iptables -A OUTPUT -p tcp -d api.geekdo.com --dport 443 -j ACCEPT
iptables -A OUTPUT -p tcp -d api.geekdo.com --dport 80 -j ACCEPT

# GitHub for git operations and package management
iptables -A OUTPUT -p tcp -d github.com --dport 443 -j ACCEPT
iptables -A OUTPUT -p tcp -d api.github.com --dport 443 -j ACCEPT
iptables -A OUTPUT -p tcp -d raw.githubusercontent.com --dport 443 -j ACCEPT
iptables -A OUTPUT -p tcp -d codeload.github.com --dport 443 -j ACCEPT

# Python package management (PyPI, uv, etc.)
iptables -A OUTPUT -p tcp -d pypi.org --dport 443 -j ACCEPT
iptables -A OUTPUT -p tcp -d files.pythonhosted.org --dport 443 -j ACCEPT
iptables -A OUTPUT -p tcp -d astral.sh --dport 443 -j ACCEPT

# Allow VS Code and devcontainer infrastructure
iptables -A OUTPUT -p tcp -d vscode-auth.github.com --dport 443 -j ACCEPT
iptables -A OUTPUT -p tcp -d marketplace.visualstudio.com --dport 443 -j ACCEPT

# Allow local development server (Streamlit)
iptables -A OUTPUT -p tcp --dport 8501 -j ACCEPT
iptables -A INPUT -p tcp --dport 8501 -j ACCEPT

# Log dropped packets for debugging (optional)
iptables -A OUTPUT -j LOG --log-prefix "DROPPED: " --log-level 4

echo "✅ Firewall rules configured successfully!"
echo "🌐 Allowed domains:"
echo "   - api.finna.fi (Finna API)"
echo "   - boardgamegeek.com (BGG API)" 
echo "   - github.com (Git operations)"
echo "   - pypi.org (Python packages)"
echo "   - VS Code infrastructure"
echo ""
echo "🚫 All other outbound connections are blocked by default"

# Save rules (if possible)
if command -v iptables-save >/dev/null 2>&1; then
    iptables-save > /etc/iptables/rules.v4 || true
fi