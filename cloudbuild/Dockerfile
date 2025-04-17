# This Dockerfile creates an image for running presubmit tests.
FROM ubuntu:22.04

# Set noninteractive frontend for apt to avoid prompts during build
ENV DEBIAN_FRONTEND=noninteractive

# Install prerequisites, JDKs, and Google Cloud CLI in a single RUN command
RUN apt-get update && apt-get install -y --no-install-recommends \
            ca-certificates \
            curl \
            gnupg \
    # Add Adoptium repository and key
    && curl -s https://packages.adoptium.net/artifactory/api/gpg/key/public | gpg --dearmor -o /usr/share/keyrings/adoptium.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb jammy main" | tee /etc/apt/sources.list.d/adoptium.list \
    # Add Google Cloud SDK repository and key
    && curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee /etc/apt/sources.list.d/google-cloud-sdk.list \
    # Update package list again after adding new repos
    && apt-get update \
    # Install JDKs and Google Cloud CLI
    && apt-get install -y --no-install-recommends \
        openjdk-8-jdk \
        temurin-11-jdk \
        temurin-17-jdk \
        google-cloud-cli \
    # Clean up apt cache and lists
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set a default JAVA_HOME (optional, mainly for processes outside toolchains)
# Toolchains plugin will select the correct JDK based on toolchains.xml
# Setting it to 8 to match your original base image's default
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV JAVA8_HOME=/usr/lib/jvm/java-8-openjdk-amd64
ENV JAVA11_HOME=/usr/lib/jvm/temurin-11-jdk-amd64
ENV JAVA17_HOME=/usr/lib/jvm/temurin-17-jdk-amd64
