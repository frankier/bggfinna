# Use Ubuntu 22.04 as base image for better Python 3.13+ support
FROM mcr.microsoft.com/vscode/devcontainers/base:ubuntu-22.04

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3.11 \
    python3.11-dev \
    python3.11-venv \
    python3-pip \
    curl \
    wget \
    git \
    build-essential \
    libssl-dev \
    libffi-dev \
    sqlite3 \
    libsqlite3-dev \
    zsh \
    fzf \
    ripgrep \
    fd-find \
    iptables \
    && rm -rf /var/lib/apt/lists/*

# Create symlinks for python commands
RUN ln -sf /usr/bin/python3.11 /usr/local/bin/python && \
    ln -sf /usr/bin/python3.11 /usr/local/bin/python3

# Switch to vscode user for remaining setup
USER vscode

# Install uv for vscode user
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/home/vscode/.cargo/bin:$PATH"

# Set up shell (zsh with some useful aliases)
RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" || true

# Create aliases for common commands
RUN echo 'alias ll="ls -la"' >> /home/vscode/.bashrc && \
    echo 'alias python="uv run python"' >> /home/vscode/.bashrc && \
    echo 'alias pytest="uv run pytest"' >> /home/vscode/.bashrc && \
    echo 'alias streamlit="uv run streamlit"' >> /home/vscode/.bashrc

RUN mkdir -p /workspaces/bggfinna && \
    chown -R vscode:vscode /workspaces/bggfinna

# Set working directory
WORKDIR /workspaces/bggfinna

# Copy project files and install dependencies
COPY --chown=vscode:vscode pyproject.toml uv.lock ./
RUN uv sync --locked --group test

# Expose Streamlit port
EXPOSE 8501

# Set default shell
SHELL ["/bin/bash", "-c"]
