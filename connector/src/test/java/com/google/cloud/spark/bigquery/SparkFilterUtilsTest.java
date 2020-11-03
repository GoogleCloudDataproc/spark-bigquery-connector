/*
 * Copyright 2018 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.storage.v1.DataFormat;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.sources.*;
import org.junit.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;

public class SparkFilterUtilsTest {

  private static final DataFormat ARROW = DataFormat.ARROW;
  private static final DataFormat AVRO = DataFormat.AVRO;

  @Test
  public void testValidFiltersForAvro() {
    ImmutableList<Filter> validFilters =
        ImmutableList.of(
            EqualTo.apply("foo", "manatee"),
            GreaterThan.apply("foo", "aardvark"),
            GreaterThanOrEqual.apply("bar", 2),
            LessThan.apply("foo", "zebra"),
            LessThanOrEqual.apply("bar", 1),
            In.apply("foo", new Object[] {1, 2, 3}),
            IsNull.apply("foo"),
            IsNotNull.apply("foo"),
            And.apply(IsNull.apply("foo"), IsNotNull.apply("bar")),
            Or.apply(IsNull.apply("foo"), IsNotNull.apply("foo")),
            Not.apply(IsNull.apply("foo")),
            StringStartsWith.apply("foo", "abc"),
            StringEndsWith.apply("foo", "def"),
            StringContains.apply("foo", "abcdef"));
    validFilters.forEach(f -> assertThat(SparkFilterUtils.unhandledFilters(AVRO, f)).isEmpty());
  }

  @Test
  public void testValidFiltersForArrow() {
    ImmutableList<Filter> validFilters =
        ImmutableList.of(
            EqualTo.apply("foo", "manatee"),
            GreaterThan.apply("foo", "aardvark"),
            GreaterThanOrEqual.apply("bar", 2),
            LessThan.apply("foo", "zebra"),
            LessThanOrEqual.apply("bar", 1),
            In.apply("foo", new Object[] {1, 2, 3}),
            IsNull.apply("foo"),
            IsNotNull.apply("foo"),
            And.apply(IsNull.apply("foo"), IsNotNull.apply("bar")),
            Not.apply(IsNull.apply("foo")),
            StringStartsWith.apply("foo", "abc"),
            StringEndsWith.apply("foo", "def"),
            StringContains.apply("foo", "abcdef"));
    validFilters.forEach(f -> assertThat(SparkFilterUtils.unhandledFilters(ARROW, f)).isEmpty());
  }

  @Test
  public void testMultipleValidFiltersAreHandled() {
    Filter valid1 = EqualTo.apply("foo", "bar");
    Filter valid2 = EqualTo.apply("bar", 1);
    assertThat(SparkFilterUtils.unhandledFilters(AVRO, valid1, valid2)).isEmpty();
  }

  @Test
  public void testInvalidFiltersWithAvro() {
    Filter valid1 = EqualTo.apply("foo", "bar");
    Filter valid2 = EqualTo.apply("bar", 1);
    Filter invalid1 = EqualNullSafe.apply("foo", "bar");
    Filter invalid2 =
        And.apply(EqualTo.apply("foo", "bar"), Not.apply(EqualNullSafe.apply("bar", 1)));
    Iterable<Filter> unhandled =
        SparkFilterUtils.unhandledFilters(AVRO, valid1, valid2, invalid1, invalid2);
    assertThat(unhandled).containsExactly(invalid1, invalid2);
  }

  @Test
  public void testInvalidFiltersWithArrow() {
    Filter valid1 = EqualTo.apply("foo", "bar");
    Filter valid2 = EqualTo.apply("bar", 1);
    Filter invalid1 = EqualNullSafe.apply("foo", "bar");
    Filter invalid2 =
        And.apply(EqualTo.apply("foo", "bar"), Not.apply(EqualNullSafe.apply("bar", 1)));
    Filter invalid3 = Or.apply(IsNull.apply("foo"), IsNotNull.apply("foo"));
    Iterable<Filter> unhandled =
        SparkFilterUtils.unhandledFilters(ARROW, valid1, valid2, invalid1, invalid2, invalid3);
    assertThat(unhandled).containsExactly(invalid1, invalid2, invalid3);
  }

  @Test
  public void testNewFilterBehaviourWithFilterOption() {
    checkFilters(
        AVRO, "(f>1)", "(f>1) AND (`a` > 2)", Optional.of("f>1"), GreaterThan.apply("a", 2));
  }

  @Test
  public void testNewFilterBehaviourNoFilterOption() {
    checkFilters(AVRO, "", "(`a` > 2)", Optional.empty(), GreaterThan.apply("a", 2));
  }

  private void checkFilters(
      DataFormat readDateFormat,
      String resultWithoutFilters,
      String resultWithFilters,
      Optional<String> configFilter,
      Filter... filters) {
    String result1 = SparkFilterUtils.getCompiledFilter(readDateFormat, configFilter);
    assertThat(result1).isEqualTo(resultWithoutFilters);
    String result2 = SparkFilterUtils.getCompiledFilter(readDateFormat, configFilter, filters);
    assertThat(result2).isEqualTo(resultWithFilters);
  }

  @Test
  public void testStringFilters() {
    assertThat(SparkFilterUtils.compileFilter(StringStartsWith.apply("foo", "bar")))
        .isEqualTo("`foo` LIKE 'bar%'");
    assertThat(SparkFilterUtils.compileFilter(StringEndsWith.apply("foo", "bar")))
        .isEqualTo("`foo` LIKE '%bar'");
    assertThat(SparkFilterUtils.compileFilter(StringContains.apply("foo", "bar")))
        .isEqualTo("`foo` LIKE '%bar%'");

    assertThat(SparkFilterUtils.compileFilter(StringStartsWith.apply("foo", "b'ar")))
        .isEqualTo("`foo` LIKE 'b\\'ar%'");
    assertThat(SparkFilterUtils.compileFilter(StringEndsWith.apply("foo", "b'ar")))
        .isEqualTo("`foo` LIKE '%b\\'ar'");
    assertThat(SparkFilterUtils.compileFilter(StringContains.apply("foo", "b'ar")))
        .isEqualTo("`foo` LIKE '%b\\'ar%'");
  }

  @Test
  public void testNumericAndNullFilters() {

    assertThat(SparkFilterUtils.compileFilter(EqualTo.apply("foo", 1))).isEqualTo("`foo` = 1");
    assertThat(SparkFilterUtils.compileFilter(GreaterThan.apply("foo", 2))).isEqualTo("`foo` > 2");
    assertThat(SparkFilterUtils.compileFilter(GreaterThanOrEqual.apply("foo", 3)))
        .isEqualTo("`foo` >= 3");
    assertThat(SparkFilterUtils.compileFilter(LessThan.apply("foo", 4))).isEqualTo("`foo` < 4");
    assertThat(SparkFilterUtils.compileFilter(LessThanOrEqual.apply("foo", 5)))
        .isEqualTo("`foo` <= 5");
    assertThat(SparkFilterUtils.compileFilter(In.apply("foo", new Object[] {6, 7, 8})))
        .isEqualTo("`foo` IN UNNEST([6, 7, 8])");
    assertThat(SparkFilterUtils.compileFilter(IsNull.apply("foo"))).isEqualTo("`foo` IS NULL");
    assertThat(SparkFilterUtils.compileFilter(IsNotNull.apply("foo")))
        .isEqualTo("`foo` IS NOT NULL");
    assertThat(
            SparkFilterUtils.compileFilter(And.apply(IsNull.apply("foo"), IsNotNull.apply("bar"))))
        .isEqualTo("(`foo` IS NULL) AND (`bar` IS NOT NULL)");
    assertThat(
            SparkFilterUtils.compileFilter(Or.apply(IsNull.apply("foo"), IsNotNull.apply("bar"))))
        .isEqualTo("(`foo` IS NULL) OR (`bar` IS NOT NULL)");
    assertThat(SparkFilterUtils.compileFilter(Not.apply(IsNull.apply("foo"))))
        .isEqualTo("(NOT (`foo` IS NULL))");
  }

  @Test
  public void testDateFilters() throws ParseException {
    assertThat(
            SparkFilterUtils.compileFilter(
                In.apply(
                    "datefield",
                    new Object[] {Date.valueOf("2020-09-01"), Date.valueOf("2020-11-03")})))
        .isEqualTo("`datefield` IN UNNEST([DATE '2020-09-01', DATE '2020-11-03'])");
  }

  @Test
  public void testTimestampFilters() throws ParseException {
    Timestamp ts1 = Timestamp.valueOf("2008-12-25 15:30:00");
    Timestamp ts2 = Timestamp.valueOf("2020-01-25 02:10:10");
    assertThat(SparkFilterUtils.compileFilter(In.apply("tsfield", new Object[] {ts1, ts2})))
        .isEqualTo(
            "`tsfield` IN UNNEST([TIMESTAMP '2008-12-25 15:30:00.0', TIMESTAMP '2020-01-25 02:10:10.0'])");
  }
}
