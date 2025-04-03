package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Helper class to store parsed query parameters (Named or Positional) and their mode.
 *
 * <p>Instances of this class are created by the {@link BigQueryUtil} class. This class is immutable
 * and serializable, designed for Java 8 compatibility.
 */
public final class QueryParameterHelper implements Serializable {

  private static final long serialVersionUID = 1L; // For Serializable interface

  private final ParameterMode mode;
  private final ImmutableMap<String, QueryParameterValue> namedParameters;
  private final ImmutableList<QueryParameterValue> positionalParameters;

  public QueryParameterHelper(
      ParameterMode mode,
      Map<String, QueryParameterValue> namedParameters,
      List<QueryParameterValue> positionalParameters) {
    this.mode = mode;

    // Using Guava:
    this.namedParameters =
        namedParameters != null
            ? ImmutableMap.copyOf(namedParameters)
            : ImmutableMap.<String, QueryParameterValue>of(); // Explicit type for empty map
    this.positionalParameters =
        positionalParameters != null
            ? ImmutableList.copyOf(positionalParameters)
            : ImmutableList.<QueryParameterValue>of(); // Explicit type for empty list
  }

  static QueryParameterHelper none() {
    return new QueryParameterHelper(ParameterMode.NONE, null, null);
  }

  static QueryParameterHelper named(Map<String, QueryParameterValue> namedParameters) {
    Preconditions.checkNotNull(
        namedParameters, "namedParameters map cannot be null for named mode");
    return new QueryParameterHelper(ParameterMode.NAMED, namedParameters, null);
  }

  static QueryParameterHelper positional(List<QueryParameterValue> positionalParameters) {
    Preconditions.checkNotNull(
        positionalParameters, "positionalParameters list cannot be null for positional mode");
    return new QueryParameterHelper(ParameterMode.POSITIONAL, null, positionalParameters);
  }

  public ParameterMode getMode() {
    return mode;
  }

  /**
   * Returns the named parameters.
   *
   * @return An Optional containing an immutable map of named parameters if mode is NAMED, otherwise
   *     Optional.empty().
   */
  public Optional<ImmutableMap<String, QueryParameterValue>> getNamedParameters() {
    return mode == ParameterMode.NAMED ? Optional.of(namedParameters) : Optional.empty();
  }

  /**
   * Returns the positional parameters.
   *
   * @return An Optional containing an immutable list of positional parameters if mode is
   *     POSITIONAL, otherwise Optional.empty().
   */
  public Optional<ImmutableList<QueryParameterValue>> getPositionalParameters() {
    return mode == ParameterMode.POSITIONAL ? Optional.of(positionalParameters) : Optional.empty();
  }

  /** Returns true if no parameters were defined, false otherwise. */
  public boolean isEmpty() {
    return mode == ParameterMode.NONE;
  }

  public QueryJobConfiguration.Builder configureBuilder(QueryJobConfiguration.Builder builder) {
    Preconditions.checkNotNull(builder, "QueryJobConfiguration.Builder cannot be null");

    switch (this.mode) {
      case NAMED:
        this.namedParameters.forEach(
            (paramName, paramValue) -> {
              builder.addNamedParameter(paramName, paramValue);
            });
        break;
      case POSITIONAL:
        this.positionalParameters.forEach(
            (paramValue) -> {
              builder.addPositionalParameter(paramValue);
            });
        break;
      case NONE:
        break;
    }
    return builder;
  }
}
