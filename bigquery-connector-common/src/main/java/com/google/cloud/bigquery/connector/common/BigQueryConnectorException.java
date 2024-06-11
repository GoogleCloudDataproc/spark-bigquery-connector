/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.bigquery.connector.common;

import static com.google.cloud.bigquery.connector.common.BigQueryErrorCode.BIGQUERY_INVALID_SCHEMA;
import static com.google.cloud.bigquery.connector.common.BigQueryErrorCode.UNKNOWN;

import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;

public class BigQueryConnectorException extends RuntimeException {

  final BigQueryErrorCode errorCode;

  public BigQueryConnectorException(String message) {
    this(UNKNOWN, message);
  }

  public BigQueryConnectorException(String message, StatusException cause) {
    this(message, new SerializableStatusException(cause));
  }

  public BigQueryConnectorException(String message, StatusRuntimeException cause) {
    this(message, new SerializableStatusException(cause));
  }

  public BigQueryConnectorException(String message, Throwable cause) {
    this(UNKNOWN, message, cause);
  }

  public BigQueryConnectorException(BigQueryErrorCode errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  public BigQueryConnectorException(
      BigQueryErrorCode errorCode, String message, StatusException cause) {
    this(errorCode, message, new SerializableStatusException(cause));
  }

  public BigQueryConnectorException(
      BigQueryErrorCode errorCode, String message, StatusRuntimeException cause) {
    this(errorCode, message, new SerializableStatusException(cause));
  }

  public BigQueryConnectorException(BigQueryErrorCode errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public BigQueryErrorCode getErrorCode() {
    return errorCode;
  }

  // inner child class
  public static class InvalidSchemaException extends BigQueryConnectorException {
    public InvalidSchemaException(String message) {
      super(BIGQUERY_INVALID_SCHEMA, message);
    }

    public InvalidSchemaException(String message, Throwable t) {
      super(BIGQUERY_INVALID_SCHEMA, message, t);
    }
  }

  /**
   * StatusRuntimeException contains non-serializable members causing issues when sending exceptions
   * from the executors to the driver. this class provides a serializable alternative.
   */
  public static class SerializableStatusException extends RuntimeException {
    private final String wrappedExceptionClassName;
    private final String message;
    private final StackTraceElement[] stackTrace;

    SerializableStatusException(StatusException wrapped) {
      this.wrappedExceptionClassName = wrapped.getClass().getName();
      this.message = wrapped.getMessage();
      this.stackTrace = wrapped.getStackTrace().clone();
    }

    SerializableStatusException(StatusRuntimeException wrapped) {
      this.wrappedExceptionClassName = wrapped.getClass().getName();
      this.message = wrapped.getMessage();
      this.stackTrace = wrapped.getStackTrace().clone();
    }

    @Override
    public String getMessage() {
      return this.message;
    }

    @Override
    public String toString() {
      return this.wrappedExceptionClassName + ": " + this.message;
    }

    @Override
    public StackTraceElement[] getStackTrace() {
      return this.stackTrace;
    }
  }
}
