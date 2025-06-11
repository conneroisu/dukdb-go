#!/bin/bash

echo "Testing Docker Claude Code dev container..."

# Check if .env exists
if [ ! -f .env ]; then
    echo "Creating .env file from .env.example..."
    cp .env.example .env
    echo "Please edit .env and add your ANTHROPIC_API_KEY"
    exit 1
fi

# Start the container
echo "Starting container..."
docker-compose up -d

# Wait for container to be ready
sleep 2

# Run tests
echo -e "\n--- Testing Claude Code installation ---"
docker-compose exec claude-dev claude --version || echo "Claude Code needs API key configuration"

echo -e "\n--- Testing Nix installation ---"
docker-compose exec claude-dev nix --version

echo -e "\n--- Testing Node.js installation ---"
docker-compose exec claude-dev node --version

echo -e "\n--- Testing npm installation ---"
docker-compose exec claude-dev npm --version

echo -e "\n--- Testing workspace mount ---"
docker-compose exec claude-dev ls -la /workspace

echo -e "\n--- Testing ~/.claude mount ---"
docker-compose exec claude-dev ls -la /home/claude/.claude || echo "~/.claude directory not found on host"

echo -e "\n--- Testing if flake.nix is detected ---"
docker-compose exec claude-dev test -f /workspace/flake.nix && echo "flake.nix found in workspace" || echo "No flake.nix in workspace"

echo -e "\nContainer is ready! Run 'docker-compose exec claude-dev bash' to enter."