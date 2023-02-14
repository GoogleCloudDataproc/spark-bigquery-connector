/*
 * Copyright 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery;

import com.google.common.base.Objects;

import java.time.ZoneId;

public class SchemaConvertersConfiguration {

    private final ZoneId datetimeZoneId;

    private SchemaConvertersConfiguration(ZoneId datetimeZoneId) {
        this.datetimeZoneId = datetimeZoneId;
    }

    public static SchemaConvertersConfiguration from(SparkBigQueryConfig config) {
        return new SchemaConvertersConfiguration(config.getDatetimeZoneId());
    }

    public ZoneId getDatetimeZoneId() {
        return datetimeZoneId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SchemaConvertersConfiguration that = (SchemaConvertersConfiguration) o;
        return Objects.equal(datetimeZoneId, that.datetimeZoneId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(datetimeZoneId);
    }

    @Override
    public String toString() {
        return "SchemaConvertersConfiguration{" +
                "datetimeZoneId=" + datetimeZoneId +
                '}';
    }
}
