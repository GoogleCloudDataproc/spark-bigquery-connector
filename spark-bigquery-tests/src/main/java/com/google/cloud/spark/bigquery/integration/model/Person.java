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

public class Person {

  private String name;
  private List<Friend> friends;

  public Person() {
  }

  public Person(String name,
      List<Friend> friends) {
    this.name = name;
    this.friends = friends;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<Friend> getFriends() {
    return friends;
  }

  public void setFriends(List<Friend> friends) {
    this.friends = friends;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Person)) {
      return false;
    }
    Person person = (Person) o;
    return Objects.equal(name, person.name) && Objects
        .equal(friends, person.friends);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(name, friends);
  }

  @Override
  public String toString() {
    return "Person{" +
        "name='" + name + '\'' +
        ", friends=" + friends +
        '}';
  }
}
