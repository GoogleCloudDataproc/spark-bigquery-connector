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

public class RangeData implements Serializable {

  private static final long serialVersionUID = 194460107083111775L;
  private String str;
  private Long rng;

  public RangeData(String str, Long rng) {
    this.str = str;
    this.rng = rng;
  }

  public String getStr() {
    return str;
  }

  public void setStr(String str) {
    this.str = str;
  }

  public Long getRng() {
    return rng;
  }

  public void setRng(Long rng) {
    this.rng = rng;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RangeData)) {
      return false;
    }
    RangeData data = (RangeData) o;
    return Objects.equal(str, data.str) && Objects.equal(rng, data.rng);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(str, rng);
  }

  @Override
  public String toString() {
    return "Data{" + "str='" + str + '\'' + ", rng=" + rng + '}';
  }
}
