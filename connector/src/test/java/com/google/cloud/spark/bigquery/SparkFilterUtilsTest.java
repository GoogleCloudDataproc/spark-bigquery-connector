package com.google.cloud.spark.bigquery;

import com.google.cloud.bigquery.storage.v1beta1.Storage;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.*;
import org.junit.Test;

import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;

public class SparkFilterUtilsTest {

    private static final Storage.DataFormat ARROW = Storage.DataFormat.ARROW;
    private static final Storage.DataFormat AVRO = Storage.DataFormat.AVRO;

    @Test public void testValidFiltersForAvro() {
        ImmutableList<Filter> validFilters = ImmutableList.of(
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
                StringContains.apply("foo", "abcdef")
        );
        validFilters.forEach(f -> assertThat(SparkFilterUtils.unhandledFilters(AVRO, f)).isEmpty());
    }

    @Test public void testValidFiltersForArrow() {
        ImmutableList<Filter> validFilters = ImmutableList.of(
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
                StringContains.apply("foo", "abcdef")
        );
        validFilters.forEach(f -> assertThat(SparkFilterUtils.unhandledFilters(ARROW, f)).isEmpty());
    }

    @Test public void testMultipleValidFiltersAreHandled() {
        Filter valid1 = EqualTo.apply("foo", "bar");
        Filter valid2 = EqualTo.apply("bar", 1);
        assertThat(SparkFilterUtils.unhandledFilters(AVRO, valid1, valid2)).isEmpty();
    }

    @Test public void testInvalidFiltersWithAvro() {
        Filter valid1 = EqualTo.apply("foo", "bar");
        Filter valid2 = EqualTo.apply("bar", 1);
        Filter invalid1 = EqualNullSafe.apply("foo", "bar");
        Filter invalid2 = And.apply(EqualTo.apply("foo", "bar"), Not.apply(EqualNullSafe.apply("bar", 1)));
        Iterable<Filter> unhandled = SparkFilterUtils.unhandledFilters(AVRO, valid1, valid2, invalid1, invalid2);
        assertThat(unhandled).containsExactly(invalid1, invalid2);
    }

    @Test public void testInvalidFiltersWithArrow() {
                Filter valid1 = EqualTo.apply("foo", "bar");
        Filter valid2 = EqualTo.apply("bar", 1);
        Filter invalid1 = EqualNullSafe.apply("foo", "bar");
        Filter invalid2 = And.apply(EqualTo.apply("foo", "bar"), Not.apply(EqualNullSafe.apply("bar", 1)));
        Filter invalid3 = Or.apply(IsNull.apply("foo"), IsNotNull.apply("foo"));
        Iterable<Filter> unhandled = SparkFilterUtils.unhandledFilters(ARROW, valid1, valid2, invalid1, invalid2, invalid3);
        assertThat(unhandled).containsExactly(invalid1, invalid2, invalid3);
    }
    
    @Test
    public void testNewFilterBehaviourWithFilterOption() {
        checkFilters(AVRO, "(f>1)", "(f>1) AND (`a` > 2)",
                Optional.of("f>1"), GreaterThan.apply("a", 2));
    }

    @Test
    public void testNewFilterBehaviourNoFilterOption() {
        checkFilters(AVRO, "", "(`a` > 2)",
                Optional.empty(), GreaterThan.apply("a", 2));
    }

    private void checkFilters(
            Storage.DataFormat readDateFormat,
            String resultWithoutFilters,
            String resultWithFilters, 
            Optional<String> configFilter,
            Filter... filters) {
        String result1 = SparkFilterUtils.getCompiledFilter(readDateFormat, configFilter);
        assertThat(result1).isEqualTo(resultWithoutFilters);
        String result2 = SparkFilterUtils.getCompiledFilter(readDateFormat, configFilter, filters);
        assertThat(result2).isEqualTo(resultWithFilters);
    }
}
