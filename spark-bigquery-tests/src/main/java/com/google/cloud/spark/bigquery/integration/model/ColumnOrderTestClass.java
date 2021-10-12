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

public class ColumnOrderTestClass {

  private NumStruct nums;
  private String str;

  public ColumnOrderTestClass() {}

  public ColumnOrderTestClass(NumStruct nums, String str) {
    this.nums = nums;
    this.str = str;
  }

  public NumStruct getNums() {
    return nums;
  }

  public void setNums(NumStruct nums) {
    this.nums = nums;
  }

  public String getStr() {
    return str;
  }

  public void setStr(String str) {
    this.str = str;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ColumnOrderTestClass)) {
      return false;
    }
    ColumnOrderTestClass that = (ColumnOrderTestClass) o;
    return Objects.equal(nums, that.nums) && Objects.equal(str, that.str);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nums, str);
  }

  @Override
  public String toString() {
    return "ColumnOrderTestClass{" + "nums=" + nums + ", str='" + str + '\'' + '}';
  }
}
