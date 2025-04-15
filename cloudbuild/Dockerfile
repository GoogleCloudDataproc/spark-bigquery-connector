# This Dockerfile creates an image for running presubmit tests.
FROM ubuntu:22.04

# Set noninteractive frontend for apt to avoid prompts during build
ENV DEBIAN_FRONTEND=noninteractive

# Single RUN command combining all package management steps:
# 1. Initial update
# 2. Install prerequisites (curl, gpg, ca-certs)
# 3. Add Google Cloud SDK repository key and source list
# 4. Update package list again after adding the new repo
# 5. Install desired JDKs and Google Cloud CLI
# 6. Clean up apt cache
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ca-certificates \
      curl \
      gnupg \
    && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee /etc/apt/sources.list.d/google-cloud-sdk.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
         openjdk-8-jdk \
         openjdk-11-jdk \
         openjdk-17-jdk \
         google-cloud-cli \
    && rm -rf /var/lib/apt/lists/*

# Set a default JAVA_HOME (optional, mainly for processes outside toolchains)
# Toolchains plugin will select the correct JDK based on toolchains.xml
# Setting it to 8 to match your original base image's default
ENV JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
