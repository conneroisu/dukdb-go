#!/bin/bash
set -e

# Source Nix environment
if [ -f /home/claude/.nix-profile/etc/profile.d/nix.sh ]; then
    source /home/claude/.nix-profile/etc/profile.d/nix.sh
fi

# Configure Git if environment variables are set
if [ -n "$GIT_AUTHOR_NAME" ]; then
    git config --global user.name "$GIT_AUTHOR_NAME"
fi

if [ -n "$GIT_AUTHOR_EMAIL" ]; then
    git config --global user.email "$GIT_AUTHOR_EMAIL"
fi

# Ensure .claude directory has correct permissions
if [ -d "/home/claude/.claude" ]; then
    sudo chown -R claude:claude /home/claude/.claude
fi

# Check if Claude CLI is installed and working
if command -v claude &> /dev/null; then
    echo "Claude CLI is installed and available"
    claude --version || echo "Claude CLI installed but requires API key configuration"
else
    echo "Claude CLI not found, installing..."
    npm install -g @anthropic-ai/claude-code
fi

# Check if Nix is working
echo "Nix version:"
nix --version

# If nix develop is available in the workspace, offer to enter it
if [ -f "/workspace/flake.nix" ]; then
    echo ""
    echo "Found flake.nix in workspace. You can run 'nix develop' to enter the development shell."
fi

# Execute the command passed to docker run
exec "$@"