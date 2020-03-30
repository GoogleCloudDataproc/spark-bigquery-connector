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

import com.google.cloud.bigquery.storage.v1beta1.Storage;

import java.util.Optional;

class ReadSessionCreatorConfig {
    final boolean viewsEnabled;
    final Optional<String> viewMaterializationProject;
    final Optional<String> viewMaterializationDataset;
    final int viewExpirationTimeInHours;
    final Storage.DataFormat readDataFormat;
    final int maxReadRowsRetries;
    final String viewEnabledParamName;

    ReadSessionCreatorConfig(
            boolean viewsEnabled,
            Optional<String> viewMaterializationProject,
            Optional<String> viewMaterializationDataset,
             int viewExpirationTimeInHours,
            Storage.DataFormat readDataFormat,
            int maxReadRowsRetries,
            String viewEnabledParamName) {
        this.viewsEnabled = viewsEnabled;
        this.viewMaterializationProject = viewMaterializationProject;
        this.viewMaterializationDataset = viewMaterializationDataset;
        this.viewExpirationTimeInHours = viewExpirationTimeInHours;
        this.readDataFormat = readDataFormat;
        this.maxReadRowsRetries = maxReadRowsRetries;
        this.viewEnabledParamName = viewEnabledParamName;
    }
}
