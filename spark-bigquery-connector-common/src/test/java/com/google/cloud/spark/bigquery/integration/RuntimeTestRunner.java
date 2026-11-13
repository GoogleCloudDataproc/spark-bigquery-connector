/*
 * Copyright 2026 Google Inc. All Rights Reserved.
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
package com.google.cloud.spark.bigquery.integration;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.runner.Description;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;
import org.junit.runner.notification.RunListener;
import scala.Option;
import scala.Tuple2;

/**
 * Runs integration tests inside a Spark job on a Dataproc runtime and reports one result per test.
 *
 * <p>Arguments: an optional {@code --report=<path>}, followed by the tests to run, each either a
 * fully qualified class name or {@code <class>#<method>}.
 *
 * <p>Results are reported in two ways:
 *
 * <ul>
 *   <li>If {@code --report} is given, a JUnit XML report with full stack traces is written to that
 *       path (any Hadoop file system path, e.g. {@code gs://bucket/report.xml}), for the launcher
 *       to turn into individual test results.
 *   <li>One machine-readable line per test is printed to the driver output:
 *       <pre>
 * TEST_RESULT|&lt;class&gt;#&lt;method&gt;|PASS|
 * TEST_RESULT|&lt;class&gt;#&lt;method&gt;|FAIL|&lt;exception and message&gt;
 * TEST_RESULT|&lt;class&gt;#&lt;method&gt;|SKIPPED|&lt;reason&gt;
 * TEST_RESULT|&lt;class&gt;#&lt;method&gt;|IGNORED|
 * TEST_SUMMARY|run=..|failed=..|skipped=..|ignored=..
 * </pre>
 * </ul>
 *
 * <p>Always returns normally, so that test failures are reported through the results rather than as
 * a job failure.
 */
public class RuntimeTestRunner {

  private static final String REPORT_FLAG = "--report=";
  private static final String SPARK_HADOOP_PREFIX = "spark.hadoop.";

  public static void main(String[] args) throws Exception {
    String reportPath = null;
    List<Request> requests = new ArrayList<>();
    for (String arg : args) {
      if (arg.startsWith(REPORT_FLAG)) {
        reportPath = arg.substring(REPORT_FLAG.length());
      } else if (arg.contains("#")) {
        String[] parts = arg.split("#", 2);
        requests.add(Request.method(Class.forName(parts[0]), parts[1]));
      } else {
        requests.add(Request.aClass(Class.forName(arg)));
      }
    }
    JUnitCore junit = new JUnitCore();
    junit.addListener(new ResultLinePrinter());
    JUnitXmlReport report = new JUnitXmlReport();
    junit.addListener(report);
    int run = 0;
    int failed = 0;
    int skipped = 0;
    int ignored = 0;
    for (Request request : requests) {
      Result result = junit.run(request);
      run += result.getRunCount();
      failed += result.getFailureCount();
      skipped += result.getAssumptionFailureCount();
      ignored += result.getIgnoreCount();
    }
    if (reportPath != null) {
      report.write(reportPath);
    }
    System.out.printf(
        "TEST_SUMMARY|run=%d|failed=%d|skipped=%d|ignored=%d%n", run, failed, skipped, ignored);
    System.out.flush();
    // Return instead of calling System.exit(): in YARN cluster mode, exiting before the
    // ApplicationMaster records the final status marks the application as failed and retries it.
  }

  /**
   * Returns the Hadoop configuration the Spark job uses, so that the report is written with the
   * same file system implementation as the job (e.g. the one set by {@code
   * spark.hadoop.fs.gs.impl}), which can differ from the one a plain {@code new Configuration()}
   * resolves on some runtimes.
   */
  private static Configuration hadoopConfiguration() {
    Option<SparkSession> session = SparkSession.getActiveSession();
    if (session.isEmpty()) {
      session = SparkSession.getDefaultSession();
    }
    if (session.isDefined()) {
      return session.get().sparkContext().hadoopConfiguration();
    }
    // No session left (e.g. the tests stopped it): apply the job's spark.hadoop.* properties the
    // way Spark does, rather than starting a new SparkContext.
    Configuration conf = new Configuration();
    for (Tuple2<String, String> entry : new SparkConf().getAll()) {
      if (entry._1().startsWith(SPARK_HADOOP_PREFIX)) {
        conf.set(entry._1().substring(SPARK_HADOOP_PREFIX.length()), entry._2());
      }
    }
    return conf;
  }

  private static String id(Description description) {
    return description.getClassName() + "#" + description.getMethodName();
  }

  private static class ResultLinePrinter extends RunListener {
    private final Set<String> reported = new HashSet<>();

    @Override
    public void testFailure(Failure failure) {
      print(failure.getDescription(), "FAIL", describe(failure.getException()));
    }

    @Override
    public void testAssumptionFailure(Failure failure) {
      print(failure.getDescription(), "SKIPPED", String.valueOf(failure.getMessage()));
    }

    @Override
    public void testIgnored(Description description) {
      print(description, "IGNORED", "");
    }

    @Override
    public void testFinished(Description description) {
      if (!reported.contains(id(description))) {
        print(description, "PASS", "");
      }
    }

    private void print(Description description, String status, String detail) {
      reported.add(id(description));
      System.out.println(
          "TEST_RESULT|"
              + id(description)
              + "|"
              + status
              + "|"
              + detail.replaceAll("[\\r\\n|]", " "));
      System.out.flush();
    }

    private static String describe(Throwable t) {
      StringBuilder sb = new StringBuilder();
      for (Throwable cause = t; cause != null && sb.length() < 2000; cause = cause.getCause()) {
        if (sb.length() > 0) {
          sb.append(" <- caused by: ");
        }
        sb.append(cause.getClass().getName()).append(": ").append(cause.getMessage());
        if (cause.getCause() == cause) {
          break;
        }
      }
      return sb.toString();
    }
  }

  /** Collects per-test results and writes them as a JUnit XML report, one suite per class. */
  private static class JUnitXmlReport extends RunListener {

    private static class TestCase {
      final String className;
      final String methodName;
      long startNanos = System.nanoTime();
      double seconds;
      String failureType;
      String failureMessage;
      String failureTrace;
      String skippedMessage;

      TestCase(Description description) {
        this.className = description.getClassName();
        this.methodName = String.valueOf(description.getMethodName());
      }
    }

    private final Map<String, TestCase> testCases = new LinkedHashMap<>();

    private TestCase get(Description description) {
      return testCases.computeIfAbsent(id(description), key -> new TestCase(description));
    }

    @Override
    public synchronized void testStarted(Description description) {
      get(description).startNanos = System.nanoTime();
    }

    @Override
    public synchronized void testFinished(Description description) {
      TestCase testCase = get(description);
      testCase.seconds = (System.nanoTime() - testCase.startNanos) / 1e9;
    }

    @Override
    public synchronized void testFailure(Failure failure) {
      // Class-level failures (e.g. in @BeforeClass) have no method; they are reported as a test
      // case named after the class, so that they show up in the report.
      TestCase testCase = get(failure.getDescription());
      Throwable exception = failure.getException();
      testCase.failureType = exception.getClass().getName();
      testCase.failureMessage = String.valueOf(exception.getMessage());
      StringWriter trace = new StringWriter();
      exception.printStackTrace(new PrintWriter(trace));
      testCase.failureTrace = trace.toString();
    }

    @Override
    public synchronized void testAssumptionFailure(Failure failure) {
      get(failure.getDescription()).skippedMessage = String.valueOf(failure.getMessage());
    }

    @Override
    public synchronized void testIgnored(Description description) {
      get(description).skippedMessage = "@Ignore";
    }

    synchronized void write(String path) throws IOException {
      Map<String, List<TestCase>> suites = new LinkedHashMap<>();
      for (TestCase testCase : testCases.values()) {
        suites.computeIfAbsent(testCase.className, key -> new ArrayList<>()).add(testCase);
      }
      StringBuilder xml = new StringBuilder("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
      xml.append("<testsuites>\n");
      for (Map.Entry<String, List<TestCase>> suite : suites.entrySet()) {
        int failures = 0;
        int skipped = 0;
        double seconds = 0;
        for (TestCase testCase : suite.getValue()) {
          failures += testCase.failureType != null ? 1 : 0;
          skipped += testCase.failureType == null && testCase.skippedMessage != null ? 1 : 0;
          seconds += testCase.seconds;
        }
        xml.append(
            String.format(
                "  <testsuite name=\"%s\" tests=\"%d\" failures=\"%d\" errors=\"0\" skipped=\"%d\""
                    + " time=\"%.3f\">\n",
                escape(suite.getKey()), suite.getValue().size(), failures, skipped, seconds));
        for (TestCase testCase : suite.getValue()) {
          xml.append(
              String.format(
                  "    <testcase classname=\"%s\" name=\"%s\" time=\"%.3f\"",
                  escape(testCase.className), escape(testCase.methodName), testCase.seconds));
          if (testCase.failureType != null) {
            xml.append(
                String.format(
                    ">\n      <failure type=\"%s\" message=\"%s\">%s</failure>\n    </testcase>\n",
                    escape(testCase.failureType),
                    escape(testCase.failureMessage),
                    escape(testCase.failureTrace)));
          } else if (testCase.skippedMessage != null) {
            xml.append(
                String.format(
                    ">\n      <skipped message=\"%s\"/>\n    </testcase>\n",
                    escape(testCase.skippedMessage)));
          } else {
            xml.append("/>\n");
          }
        }
        xml.append("  </testsuite>\n");
      }
      xml.append("</testsuites>\n");
      Path reportPath = new Path(path);
      FileSystem fs = reportPath.getFileSystem(hadoopConfiguration());
      try (OutputStream out = fs.create(reportPath, true)) {
        out.write(xml.toString().getBytes(StandardCharsets.UTF_8));
      }
    }

    private static String escape(String s) {
      StringBuilder sb = new StringBuilder(s.length());
      for (char c : s.toCharArray()) {
        switch (c) {
          case '&':
            sb.append("&amp;");
            break;
          case '<':
            sb.append("&lt;");
            break;
          case '>':
            sb.append("&gt;");
            break;
          case '"':
            sb.append("&quot;");
            break;
          default:
            if (c < 0x20 && c != '\n' && c != '\r' && c != '\t') {
              sb.append(' ');
            } else {
              sb.append(c);
            }
        }
      }
      return sb.toString();
    }
  }
}
