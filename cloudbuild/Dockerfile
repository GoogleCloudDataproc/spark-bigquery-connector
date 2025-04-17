# This Dockerfile creates an image for running presubmit tests.
# Uses modern apt key management AND creates the necessary directory.
FROM eclipse-temurin:17

# Install dependencies needed for adding repo and cleanup
# (Separating this step is good practice)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg \
    apt-transport-https \
    ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Add Google Cloud SDK repository and install CLI
RUN \
  # Create directories for keyrings AND sources list (THE FIX)
  mkdir -p /usr/share/keyrings && \
  mkdir -p /etc/apt/sources.list.d && \
  # Download the key, dearmor it, and save to the keyring directory
  curl -fsSL https://packages.cloud.google.com/apt/doc/apt-key.gpg | \
  gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && \
  # Add the repository, pointing to the key using signed-by
  echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | \
  tee /etc/apt/sources.list.d/google-cloud-sdk.list && \
  # Update package list and install the CLI
  apt-get update && \
  apt-get install -y google-cloud-cli && \
  # Clean up apt cache
  rm -rf /var/lib/apt/lists/*
