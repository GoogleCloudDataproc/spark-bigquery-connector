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

public class StringStruct {

  private String str3;
  private String str1;
  private String str2;

  public StringStruct() {
  }

  public StringStruct(String str3, String str1, String str2) {
    this.str3 = str3;
    this.str1 = str1;
    this.str2 = str2;
  }

  public String getStr3() {
    return str3;
  }

  public void setStr3(String str3) {
    this.str3 = str3;
  }

  public String getStr1() {
    return str1;
  }

  public void setStr1(String str1) {
    this.str1 = str1;
  }

  public String getStr2() {
    return str2;
  }

  public void setStr2(String str2) {
    this.str2 = str2;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StringStruct)) {
      return false;
    }
    StringStruct that = (StringStruct) o;
    return Objects.equal(str3, that.str3) && Objects
        .equal(str1, that.str1) && Objects.equal(str2, that.str2);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(str3, str1, str2);
  }

  @Override
  public String toString() {
    return "StringStruct{" +
        "str3='" + str3 + '\'' +
        ", str1='" + str1 + '\'' +
        ", str2='" + str2 + '\'' +
        '}';
  }
}
