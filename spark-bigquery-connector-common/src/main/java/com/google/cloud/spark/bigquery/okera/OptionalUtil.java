package com.google.cloud.spark.bigquery.okera;

import java.util.Optional;
import java.util.function.Supplier;

public class OptionalUtil {
  public static <T> Optional<? extends T> or(
      Optional<T> o, Supplier<? extends Optional<? extends T>> fallback) {
    if (o.isPresent()) {
      return o;
    }
    return fallback.get();
  }
}
