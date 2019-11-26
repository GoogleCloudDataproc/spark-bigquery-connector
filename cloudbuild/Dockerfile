# This Dockerfile creates an image for running presubmit tests.
FROM openjdk:8

# This should match with sbt version under project/build.properties
ARG SBT_VERSION=0.13.17

# Install sbt
RUN \
  curl -L -o sbt-$SBT_VERSION.deb http://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
  dpkg -i sbt-$SBT_VERSION.deb && \
  rm sbt-$SBT_VERSION.deb && \
  apt-get update && \
  apt-get install sbt && \
  sbt sbtVersion
