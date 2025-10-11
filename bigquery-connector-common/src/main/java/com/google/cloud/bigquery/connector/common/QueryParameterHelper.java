/*
 * Copyright 2025 Google Inc. All Rights Reserved.
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

  private static final long serialVersionUID = -283738274937293739L; // For Serializable interface

  private final ParameterMode mode;
  private final ImmutableMap<String, QueryParameterValue> namedParameters;
  private final ImmutableList<QueryParameterValue> positionalParameters;

  public QueryParameterHelper(
      ParameterMode mode,
      ImmutableMap<String, QueryParameterValue> named,
      ImmutableList<QueryParameterValue> positional) {
    this.mode = mode;

    this.namedParameters = named;
    this.positionalParameters = positional;
  }

  static QueryParameterHelper none() {
    return new QueryParameterHelper(ParameterMode.NONE, ImmutableMap.of(), ImmutableList.of());
  }

  static QueryParameterHelper named(Map<String, QueryParameterValue> namedParameters) {
    Preconditions.checkNotNull(
        namedParameters, "Input named parameters map cannot be null for named mode");
    return new QueryParameterHelper(
        ParameterMode.NAMED, ImmutableMap.copyOf(namedParameters), ImmutableList.of());
  }

  static QueryParameterHelper positional(List<QueryParameterValue> positionalParameters) {
    Preconditions.checkNotNull(
        positionalParameters,
        "Input positional parameters list cannot be null for positional mode");

    return new QueryParameterHelper(
        ParameterMode.POSITIONAL,
        ImmutableMap.of(), // Pass empty immutable map
        ImmutableList.copyOf(positionalParameters));
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

    this.namedParameters.forEach(
        (paramName, paramValue) -> {
          builder.addNamedParameter(paramName, paramValue);
        });
    this.positionalParameters.forEach(
        (paramValue) -> {
          builder.addPositionalParameter(paramValue);
        });

    return builder;
  }
}
