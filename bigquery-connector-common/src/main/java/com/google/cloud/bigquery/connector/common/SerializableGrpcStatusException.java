/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigquery.connector.common;

import io.grpc.Status;

public class SerializableGrpcStatusException extends RuntimeException {
  private static final long serialVersionUID = 1L;

  private final Status.Code statusCode;
  private final String statusDescription;
  private final String causeMessage;

  public SerializableGrpcStatusException(
      String message, Status.Code statusCode, String statusDescription, String causeMessage) {
    super(message);
    this.statusCode = statusCode;
    this.statusDescription = statusDescription;
    this.causeMessage = causeMessage;
  }

  public Status.Code getStatusCode() {
    return statusCode;
  }

  public String getStatusDescription() {
    return statusDescription;
  }

  public String getCauseMessage() {
    return causeMessage;
  }

  @Override
  public String toString() {
    String base =
        super.toString() + " [Code: " + statusCode + ", Description: " + statusDescription + "]";
    if (causeMessage != null) {
      return base + " Cause: " + causeMessage;
    }
    return base;
  }
}
