/*
 * Copyright 2022 Google Inc. All Rights Reserved.
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

import java.util.Date;

/**
 * As the com.google.auth.oauth2.AccessToken class is shaded in the final jar, this class provides
 * the same functionality but maintains its package.
 */
public class AccessToken extends com.google.auth.oauth2.AccessToken {

  /**
   * @param tokenValue String representation of the access token.
   * @param expirationTime Time when access token will expire.
   */
  public AccessToken(String tokenValue, Date expirationTime) {
    super(tokenValue, expirationTime);
  }
}
