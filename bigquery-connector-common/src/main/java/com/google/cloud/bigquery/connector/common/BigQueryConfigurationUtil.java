/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigquery.connector.common;

import static com.google.cloud.bigquery.connector.common.BigQueryUtil.firstPresent;
import static com.google.cloud.bigquery.connector.common.BigQueryUtil.parseTableId;
import static java.lang.String.format;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.TableId;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/** Utilities to read configuration options */
public class BigQueryConfigurationUtil {

  public static final Supplier<com.google.common.base.Optional<String>> DEFAULT_FALLBACK =
      () -> empty();

  private BigQueryConfigurationUtil() {}

  public static com.google.common.base.Supplier<String> defaultBilledProject() {
    return () -> BigQueryOptions.getDefaultInstance().getProjectId();
  }

  public static String getRequiredOption(Map<String, String> options, String name) {
    return getOption(options, name, DEFAULT_FALLBACK)
        .toJavaUtil()
        .orElseThrow(() -> new IllegalArgumentException(format("Option %s required.", name)));
  }

  public static String getRequiredOption(
      Map<String, String> options, String name, com.google.common.base.Supplier<String> fallback) {
    return getOption(options, name, DEFAULT_FALLBACK).or(fallback);
  }

  public static com.google.common.base.Optional<String> getOption(
      Map<String, String> options, String name) {
    return getOption(options, name, DEFAULT_FALLBACK);
  }

  public static com.google.common.base.Optional<String> getOption(
      Map<String, String> options, String name, Supplier<Optional<String>> fallback) {
    return fromJavaUtil(
        firstPresent(
            java.util.Optional.ofNullable(options.get(name.toLowerCase())),
            fallback.get().toJavaUtil()));
  }

  public static com.google.common.base.Optional<String> getOptionFromMultipleParams(
      Map<String, String> options,
      Collection<String> names,
      Supplier<com.google.common.base.Optional<String>> fallback) {
    return names.stream()
        .map(name -> getOption(options, name))
        .filter(com.google.common.base.Optional::isPresent)
        .findFirst()
        .orElseGet(fallback);
  }

  public static Map<String, String> getMapEntriesWithPrefix(
      Map<String, String> map, String prefix) {
    Map<String, String> result = new HashMap();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      if (entry.getKey().startsWith(prefix)) {
        result.put(entry.getKey(), entry.getValue());
      }
    }
    return result;
  }

  public static com.google.common.base.Optional<Map<String, String>> removePrefixFromMapKeys(
      com.google.common.base.Optional<Map<String, String>> map, String prefix) {
    Map<String, String> modifiedMap = new HashMap<>();
    if (map.isPresent()) {
      for (Map.Entry<String, String> entry : map.get().entrySet()) {
        String originalKey = entry.getKey();
        String modifiedKey =
            originalKey.startsWith(prefix) ? originalKey.substring(prefix.length()) : originalKey;
        modifiedMap.put(modifiedKey, entry.getValue());
      }
    }
    return com.google.common.base.Optional.of(modifiedMap);
  }

  public static com.google.common.base.Optional<Map<String, String>> getAnyOptionsWithPrefix(
      ImmutableMap<String, String> globalOptions, Map<String, String> options, String prefix) {
    Map<String, String> result = getMapEntriesWithPrefix(globalOptions, prefix);
    Map<String, String> prefixOptions = getMapEntriesWithPrefix(options, prefix);
    result.putAll(prefixOptions);
    return com.google.common.base.Optional.of(result);
  }

  public static com.google.common.base.Optional<String> getAnyOption(
      ImmutableMap<String, String> globalOptions, Map<String, String> options, String name) {
    return com.google.common.base.Optional.fromNullable(options.get(name.toLowerCase()))
        .or(com.google.common.base.Optional.fromNullable(globalOptions.get(name)));
  }

  // gives the option to support old configurations as fallback
  // Used to provide backward compatibility
  public static com.google.common.base.Optional<String> getAnyOption(
      ImmutableMap<String, String> globalOptions,
      Map<String, String> options,
      Collection<String> names) {
    return names.stream()
        .map(name -> getAnyOption(globalOptions, options, name))
        .filter(optional -> optional.isPresent())
        .findFirst()
        .orElse(empty());
  }

  public static boolean getAnyBooleanOption(
      ImmutableMap<String, String> globalOptions,
      Map<String, String> options,
      String name,
      boolean defaultValue) {
    return getAnyOption(globalOptions, options, name).transform(Boolean::valueOf).or(defaultValue);
  }

  public static com.google.common.base.Optional empty() {
    return com.google.common.base.Optional.absent();
  }

  public static com.google.common.base.Optional fromJavaUtil(java.util.Optional o) {
    return com.google.common.base.Optional.fromJavaUtil(o);
  }

  /** TableId that does not include partition decorator */
  public static TableId parseSimpleTableId(
      Map<String, String> options,
      Optional<String> fallbackProject,
      Optional<String> fallbackDataset) {
    String tableParam =
        getOptionFromMultipleParams(options, ImmutableList.of("table", "path"), DEFAULT_FALLBACK)
            .get();
    Optional<String> datasetParam = getOption(options, "dataset").or(fallbackDataset);
    Optional<String> projectParam = getOption(options, "project").or(fallbackProject);
    return parseTableId(
        tableParam,
        datasetParam.toJavaUtil(),
        projectParam.toJavaUtil(), /* datePartition */
        java.util.Optional.empty());
  }
}
