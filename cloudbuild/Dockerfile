# This Dockerfile creates an image for running presubmit tests.
FROM openjdk:17

RUN \
  # Create directory for apt keyrings if it doesn't exist
  mkdir -p /usr/share/keyrings && \
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
