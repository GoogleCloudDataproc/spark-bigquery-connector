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
import java.util.List;

public class NumStruct {

  private Long num3;
  private Long num2;
  private Long num1;
  private List<StringStruct> strings;

  public NumStruct() {
  }

  public NumStruct(Long num3, Long num2, Long num1, List<StringStruct> strings) {
    this.num3 = num3;
    this.num2 = num2;
    this.num1 = num1;
    this.strings = strings;
  }

  public Long getNum3() {
    return num3;
  }

  public void setNum3(Long num3) {
    this.num3 = num3;
  }

  public Long getNum2() {
    return num2;
  }

  public void setNum2(Long num2) {
    this.num2 = num2;
  }

  public Long getNum1() {
    return num1;
  }

  public void setNum1(Long num1) {
    this.num1 = num1;
  }

  public List<StringStruct> getStrings() {
    return strings;
  }

  public void setStrings(List<StringStruct> strings) {
    this.strings = strings;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof NumStruct)) {
      return false;
    }
    NumStruct numStruct = (NumStruct) o;
    return Objects.equal(num3, numStruct.num3)
        && Objects.equal(num2, numStruct.num2)
        && Objects.equal(num1, numStruct.num1)
        && Objects.equal(strings, numStruct.strings);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(num3, num2, num1, strings);
  }

  @Override
  public String toString() {
    return "NumStruct{" +
        "num3=" + num3 +
        ", num2=" + num2 +
        ", num1=" + num1 +
        ", stringStructArr=" + strings +
        '}';
  }
}
