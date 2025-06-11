# Docker Development Environment

This Docker setup provides a development container with Claude Code and Nix installed, sharing your local ~/.claude directory.

## Prerequisites

- Docker and Docker Compose installed
- Your ANTHROPIC_API_KEY

## Setup

1. Copy the `.env.example` to `.env` and fill in your details:
   ```bash
   cp .env.example .env
   # Edit .env with your ANTHROPIC_API_KEY and Git configuration
   ```

2. Build and start the container:
   ```bash
   docker-compose up -d --build
   ```

3. Enter the container:
   ```bash
   docker-compose exec claude-dev bash
   ```

## Features

- **Claude Code CLI**: Pre-installed and configured
- **Nix Package Manager**: Available for development environments
- **Shared ~/.claude**: Your local Claude configuration is mounted
- **Persistent Nix Store**: Faster rebuilds with cached Nix packages
- **Current Directory Mounted**: The project directory is available at `/workspace`

## Usage

Once inside the container:

```bash
# Check Claude Code
claude --version

# Check Nix
nix --version

# If the project has a flake.nix, enter the development shell
nix develop

# Run Claude Code commands
claude "Help me understand this codebase"
```

## Stopping the Container

```bash
docker-compose down
```

## Troubleshooting

- If Claude Code is not available, check that your ANTHROPIC_API_KEY is set in the .env file
- For permission issues with ~/.claude, the entrypoint script automatically fixes ownership
- Nix packages are cached in Docker volumes for faster subsequent starts