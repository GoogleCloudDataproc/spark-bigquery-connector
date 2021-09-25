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

public class Friend {

  private int age;
  private List<Link> links;

  public Friend() {}

  public Friend(int age, List<Link> links) {
    this.age = age;
    this.links = links;
  }

  public int getAge() {
    return age;
  }

  public void setAge(int age) {
    this.age = age;
  }

  public List<Link> getLinks() {
    return links;
  }

  public void setLinks(List<Link> links) {
    this.links = links;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Friend)) {
      return false;
    }
    Friend friend = (Friend) o;
    return age == friend.age && Objects.equal(links, friend.links);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(age, links);
  }

  @Override
  public String toString() {
    return "Friend{" + "age=" + age + ", links=" + links + '}';
  }
}
