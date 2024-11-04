# Use a lightweight base image
FROM python:3.11-slim

ENV PIP_DEFAULT_TIMEOUT=100 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

# Install CA certificates, gcc, python3-dev, and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    unzip \
    procps \
    ca-certificates \
    gcc \
    python3-dev && \
    rm -rf /var/lib/apt/lists/*

# Add support for multiple architectures (specifically for ARM64)
RUN dpkg --add-architecture amd64 && \
    apt-get update && \
    apt-get install -y libc6:amd64 && \
    rm -rf /var/lib/apt/lists/*

# Upgrade pip and install synapseclient
RUN pip install --no-cache-dir --upgrade pip && \
    pip install synapseclient==4.6.0

# Download and install AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# Set the working directory
WORKDIR /workspace