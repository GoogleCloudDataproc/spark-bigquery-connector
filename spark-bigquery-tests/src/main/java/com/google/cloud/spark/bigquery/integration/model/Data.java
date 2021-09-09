/*
 * Copyright 2021 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.integration.model;

import com.google.common.base.Objects;
import java.io.Serializable;
import java.sql.Timestamp;

public class Data implements Serializable {

  private String str;
  private java.sql.Timestamp ts;

  public Data(String str, Timestamp ts) {
    this.str = str;
    this.ts = ts;
  }

  public String getStr() {
    return str;
  }

  public void setStr(String str) {
    this.str = str;
  }

  public Timestamp getTs() {
    return ts;
  }

  public void setTs(Timestamp ts) {
    this.ts = ts;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Data)) {
      return false;
    }
    Data data = (Data) o;
    return Objects.equal(str, data.str) && Objects
        .equal(ts, data.ts);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(str, ts);
  }

  @Override
  public String toString() {
    return "Data{" +
        "str='" + str + '\'' +
        ", ts=" + ts +
        '}';
  }
}

