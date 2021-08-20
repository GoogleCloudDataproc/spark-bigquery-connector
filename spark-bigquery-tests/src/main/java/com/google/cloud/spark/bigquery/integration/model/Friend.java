package com.google.cloud.spark.bigquery.integration.model;

import com.google.common.base.Objects;
import java.util.List;

public class Friend {

  private int age;
  private List<Link> links;

  public Friend() {
  }

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
    return "Friend{" +
        "age=" + age +
        ", links=" + links +
        '}';
  }
}
