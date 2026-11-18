/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.integration;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class IntegrationTestUtilsTest {

  private static final RetrySettings FAST_RETRY_SETTINGS =
      RetrySettings.newBuilder()
          .setMaxAttempts(4)
          .setInitialRetryDelayDuration(Duration.ZERO)
          .setRetryDelayMultiplier(1.0)
          .setMaxRetryDelayDuration(Duration.ZERO)
          .setTotalTimeoutDuration(Duration.ofSeconds(10))
          .build();

  @Test
  public void testRunWithRetries_succeedsAfterTransientHttp503() {
    AtomicInteger attempts = new AtomicInteger(0);
    String result =
        IntegrationTestUtils.runWithRetries(
            () -> {
              if (attempts.incrementAndGet() < 3) {
                throw new BigQueryException(
                    503,
                    "Error encountered during execution. Retrying may solve the problem.",
                    new BigQueryError(
                        "backendError",
                        null,
                        "Error encountered during execution. Retrying may solve the problem."));
              }
              return "ok";
            },
            FAST_RETRY_SETTINGS,
            "testOp");

    assertThat(result).isEqualTo("ok");
    assertThat(attempts.get()).isEqualTo(3);
  }

  @Test
  public void testRunWithRetries_succeedsAfterTransientJobError() {
    AtomicInteger attempts = new AtomicInteger(0);
    String result =
        IntegrationTestUtils.runWithRetries(
            () -> {
              int current = attempts.incrementAndGet();
              if (current == 1) {
                throw new BigQueryException(
                    Collections.singletonList(
                        new BigQueryError("backendError", null, "Backend error")));
              }
              if (current == 2) {
                throw new BigQueryException(
                    Collections.singletonList(
                        new BigQueryError("rateLimitExceeded", null, "Rate limit exceeded")));
              }
              return "ok";
            },
            FAST_RETRY_SETTINGS,
            "testOp");

    assertThat(result).isEqualTo("ok");
    assertThat(attempts.get()).isEqualTo(3);
  }

  @Test
  public void testRunWithRetries_succeedsAfterHttp429() {
    AtomicInteger attempts = new AtomicInteger(0);
    String result =
        IntegrationTestUtils.runWithRetries(
            () -> {
              if (attempts.incrementAndGet() == 1) {
                throw new BigQueryException(429, "Too Many Requests");
              }
              return "ok";
            },
            FAST_RETRY_SETTINGS,
            "testOp");

    assertThat(result).isEqualTo("ok");
    assertThat(attempts.get()).isEqualTo(2);
  }

  @Test
  public void testRunWithRetries_failsAfterMaxAttempts() {
    AtomicInteger attempts = new AtomicInteger(0);
    BigQueryException thrown =
        assertThrows(
            BigQueryException.class,
            () ->
                IntegrationTestUtils.runWithRetries(
                    () -> {
                      attempts.incrementAndGet();
                      throw new BigQueryException(503, "Service Unavailable");
                    },
                    FAST_RETRY_SETTINGS,
                    "testOp"));

    assertThat(attempts.get()).isEqualTo(4);
    assertThat(thrown.getCode()).isEqualTo(503);
  }

  @Test
  public void testRunWithRetries_doesNotRetryNonTransientBigQueryErrors() {
    BigQueryException[] nonTransientErrors =
        new BigQueryException[] {
          new BigQueryException(400, "Syntax error", new BigQueryError("invalidQuery", null, "")),
          new BigQueryException(403, "Access denied", new BigQueryError("accessDenied", null, "")),
          new BigQueryException(
              403, "Quota exceeded", new BigQueryError("quotaExceeded", null, "")),
          new BigQueryException(404, "Not found", new BigQueryError("notFound", null, "")),
          new BigQueryException(409, "Already exists", new BigQueryError("duplicate", null, ""))
        };

    for (BigQueryException error : nonTransientErrors) {
      AtomicInteger attempts = new AtomicInteger(0);
      BigQueryException thrown =
          assertThrows(
              BigQueryException.class,
              () ->
                  IntegrationTestUtils.runWithRetries(
                      () -> {
                        attempts.incrementAndGet();
                        throw error;
                      },
                      FAST_RETRY_SETTINGS,
                      "testOp"));

      assertThat(attempts.get()).isEqualTo(1);
      assertThat(thrown).isSameInstanceAs(error);
    }
  }

  @Test
  public void testRunWithRetries_doesNotRetryNonBigQueryException() {
    AtomicInteger attempts = new AtomicInteger(0);
    IllegalArgumentException thrown =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                IntegrationTestUtils.runWithRetries(
                    () -> {
                      attempts.incrementAndGet();
                      throw new IllegalArgumentException("bad argument");
                    },
                    FAST_RETRY_SETTINGS,
                    "testOp"));

    assertThat(attempts.get()).isEqualTo(1);
    assertThat(thrown.getMessage()).isEqualTo("bad argument");
  }

  @Test
  public void testRunWithRetries_restoresInterruptFlagOnInterruptedException() {
    AtomicInteger attempts = new AtomicInteger(0);
    try {
      RuntimeException thrown =
          assertThrows(
              RuntimeException.class,
              () ->
                  IntegrationTestUtils.runWithRetries(
                      () -> {
                        attempts.incrementAndGet();
                        throw new InterruptedException("interrupted");
                      },
                      FAST_RETRY_SETTINGS,
                      "testOp"));

      assertThat(attempts.get()).isEqualTo(1);
      assertThat(thrown.getCause()).isInstanceOf(InterruptedException.class);
      assertThat(Thread.currentThread().isInterrupted()).isTrue();
    } finally {
      // Clear interrupt status so it does not affect subsequent tests in this thread.
      Thread.interrupted();
    }
  }

  @Test
  public void testIsTransientBigQueryError() {
    assertThat(IntegrationTestUtils.isTransientBigQueryError(new BigQueryException(500, "500")))
        .isTrue();
    assertThat(IntegrationTestUtils.isTransientBigQueryError(new BigQueryException(502, "502")))
        .isTrue();
    assertThat(IntegrationTestUtils.isTransientBigQueryError(new BigQueryException(503, "503")))
        .isTrue();
    assertThat(IntegrationTestUtils.isTransientBigQueryError(new BigQueryException(504, "504")))
        .isTrue();
    assertThat(IntegrationTestUtils.isTransientBigQueryError(new BigQueryException(429, "429")))
        .isTrue();
    assertThat(
            IntegrationTestUtils.isTransientBigQueryError(
                new BigQueryException(new SocketTimeoutException("read timed out"))))
        .isTrue();
    assertThat(
            IntegrationTestUtils.isTransientBigQueryError(
                new BigQueryException(
                    Collections.singletonList(new BigQueryError("internalError", null, "msg")))))
        .isTrue();

    assertThat(IntegrationTestUtils.isTransientBigQueryError(null)).isFalse();
    assertThat(IntegrationTestUtils.isTransientBigQueryError(new RuntimeException("boom")))
        .isFalse();
    assertThat(IntegrationTestUtils.isTransientBigQueryError(new BigQueryException(400, "400")))
        .isFalse();
    assertThat(
            IntegrationTestUtils.isTransientBigQueryError(
                new BigQueryException(0, "interrupted", new InterruptedException())))
        .isFalse();
  }
}
