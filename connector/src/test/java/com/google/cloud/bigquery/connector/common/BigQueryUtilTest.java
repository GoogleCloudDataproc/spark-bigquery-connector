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
package com.google.cloud.bigquery.connector.common;

import com.google.cloud.bigquery.TableId;
import org.junit.Test;

import java.util.Optional;

import static com.google.common.truth.Truth.assertThat;

public class BigQueryUtilTest {

    @Test
    public void testParseTableId() {
        assertThat(BigQueryUtil.parseTableId("t", Optional.of("D"), Optional.of("P")))
                .isEqualTo(TableId.of("P", "D", "t"));
        assertThat(BigQueryUtil.parseTableId("d.t", Optional.of("D"), Optional.of("P")))
                .isEqualTo(TableId.of("P", "d", "t"));
        assertThat(BigQueryUtil.parseTableId("d.t", Optional.empty(), Optional.of("P")))
                .isEqualTo(TableId.of("P", "d", "t"));
        assertThat(BigQueryUtil.parseTableId("d.t", Optional.empty(), Optional.empty()))
                .isEqualTo(TableId.of("d", "t"));
        assertThat(BigQueryUtil.parseTableId("p.d.t", Optional.of("D"), Optional.of("P")))
                .isEqualTo(TableId.of("p", "d", "t"));
        assertThat(BigQueryUtil.parseTableId("p.d.t", Optional.empty(), Optional.empty()))
                .isEqualTo(TableId.of("p", "d", "t"));
        assertThat(BigQueryUtil.parseTableId("p:d.t", Optional.of("D"), Optional.of("P")))
                .isEqualTo(TableId.of("p", "d", "t"));
        assertThat(BigQueryUtil.parseTableId("p:d.t", Optional.empty(), Optional.empty()))
                .isEqualTo(TableId.of("p", "d", "t"));
    }

}
