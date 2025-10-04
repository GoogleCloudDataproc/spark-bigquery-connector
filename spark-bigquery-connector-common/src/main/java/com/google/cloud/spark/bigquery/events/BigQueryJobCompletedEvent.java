/*
 * Copyright 2023 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.events;

import com.google.cloud.bigquery.JobConfiguration;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Optional;
import org.apache.spark.scheduler.SparkListenerEvent;

public abstract class BigQueryJobCompletedEvent implements SparkListenerEvent, Serializable {

  private static final long serialVersionUID = 5831270570484516631L;
  protected final JobInfo jobInfo;

  protected BigQueryJobCompletedEvent(JobInfo jobInfo, JobConfiguration.Type expectedType) {
    Preconditions.checkArgument(
        expectedType.equals(jobInfo.getConfiguration().getType()),
        "Created event of the wrong type, expected to be "
            + expectedType
            + ", got "
            + jobInfo.getConfiguration().getType());
    this.jobInfo = jobInfo;
  }

  public static Optional<BigQueryJobCompletedEvent> from(JobInfo completedJob) {
    switch (completedJob.getConfiguration().getType()) {
      case QUERY:
        return Optional.of(new QueryJobCompletedEvent(completedJob));
      case LOAD:
        return Optional.of(new LoadJobCompletedEvent(completedJob));
      default:
        return Optional.empty();
    }
  }

  public JobInfo getJobInfo() {
    return jobInfo;
  }

  public String getEtag() {
    return jobInfo.getEtag();
  }

  public String getGeneratedId() {
    return jobInfo.getGeneratedId();
  }

  public JobId getJobId() {
    return jobInfo.getJobId();
  }

  public String getSelfLink() {
    return jobInfo.getSelfLink();
  }

  public JobStatus getStatus() {
    return jobInfo.getStatus();
  }

  public String getUserEmail() {
    return jobInfo.getUserEmail();
  }

  @Override
  public boolean logEvent() {
    return false;
  }
}
