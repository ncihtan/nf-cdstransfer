# Use a lightweight base image
FROM debian:bullseye-slim

# Install CA certificates and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    unzip \
    procps \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Download and install AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# Set the working directory
WORKDIR /workspace

