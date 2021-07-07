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

import static com.google.cloud.bigquery.connector.common.BigQueryErrorCode.UNKNOWN;

public class BigQueryConnectorException extends RuntimeException {

  final BigQueryErrorCode errorCode;

  public BigQueryConnectorException(String message) {
    this(UNKNOWN, message);
  }

  public BigQueryConnectorException(String message, Throwable cause) {
    this(UNKNOWN, message, cause);
  }

  public BigQueryConnectorException(BigQueryErrorCode errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  public BigQueryConnectorException(BigQueryErrorCode errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public BigQueryErrorCode getErrorCode() {
    return errorCode;
  }
}
