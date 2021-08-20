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
