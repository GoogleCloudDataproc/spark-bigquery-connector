package com.google.cloud.bigquery.connector.common;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;

public class ComparisonResult {
  private static final ComparisonResult EQUAL = new ComparisonResult(null);

  private static final ComparisonResult DIFFERENT_NO_DESCRIPTION =
      new ComparisonResult(ImmutableList.of());
  private final ImmutableList<String> facts;

  static ComparisonResult fromEqualsResult(boolean equal) {
    return equal ? EQUAL : DIFFERENT_NO_DESCRIPTION;
  }

  static ComparisonResult differentWithDescription(List<String> facts) {
    return new ComparisonResult(ImmutableList.copyOf(facts));
  }

  static ComparisonResult equal() {
    return EQUAL;
  }

  static ComparisonResult differentNoDescription() {
    return DIFFERENT_NO_DESCRIPTION;
  }

  private ComparisonResult(ImmutableList<String> facts) {
    this.facts = facts;
  }

  public boolean valuesAreEqual() {
    return this.facts == null;
  }

  public String makeMessage() {
    if (this.facts == null) {
      return "";
    }
    return String.join(", ", facts);
  }

  public String toString() {
    return Objects.toString(facts);
  }
}
