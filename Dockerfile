FROM ubuntu:24.04

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV NIX_VERSION=2.24.10

# Install base dependencies
RUN apt-get update && apt-get install -y \
    curl \
    wget \
    git \
    sudo \
    xz-utils \
    ca-certificates \
    locales \
    && rm -rf /var/lib/apt/lists/*

# Set up locale
RUN locale-gen en_US.UTF-8
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

# Create a non-root user for running Claude Code
RUN useradd -m -s /bin/bash claude && \
    echo "claude ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Install Nix as claude user
USER claude
WORKDIR /home/claude

# Install Nix in single-user mode
RUN curl -L https://nixos.org/nix/install | sh -s -- --no-daemon

# Source Nix profile
ENV PATH="/home/claude/.nix-profile/bin:${PATH}"
ENV NIX_PATH="/home/claude/.nix-defexpr/channels"

# Install Node.js (includes npm)
RUN . /home/claude/.nix-profile/etc/profile.d/nix.sh && \
    nix-env -i nodejs

# Set npm prefix to user directory
RUN mkdir -p /home/claude/.npm-global && \
    npm config set prefix '/home/claude/.npm-global'

# Add npm global bin to PATH
ENV PATH="/home/claude/.npm-global/bin:${PATH}"

# Install Claude Code CLI
RUN . /home/claude/.nix-profile/etc/profile.d/nix.sh && \
    npm install -g @anthropic-ai/claude-code

# Switch back to root for final setup
USER root

# Create workspace directory
RUN mkdir -p /workspace && chown claude:claude /workspace

# Create entrypoint script
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

# Switch back to claude user
USER claude
WORKDIR /workspace

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["bash"]