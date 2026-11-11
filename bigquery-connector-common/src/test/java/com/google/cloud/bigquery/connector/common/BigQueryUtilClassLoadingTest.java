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
package com.google.cloud.bigquery.connector.common;

import static com.google.common.truth.Truth.assertThat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertThrows;
import static org.junit.Assume.assumeTrue;

import com.google.auth.Credentials;
import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests that user-supplied classes are loaded from the thread context class loader, as happens when
 * they come from the jars added to the application while the connector is on the cluster's
 * classpath.
 */
public class BigQueryUtilClassLoadingTest {

  private static final String HIDDEN_PROVIDER_CLASS_NAME =
      "com.example.hidden.HiddenAccessTokenProvider";

  // Compiled at test time into a directory that is only visible to a dedicated class loader, so
  // that the connector's own class loader can't find it.
  private static final String HIDDEN_PROVIDER_SOURCE =
      "package com.example.hidden;\n"
          + "\n"
          + "import com.google.cloud.bigquery.connector.common.AccessToken;\n"
          + "import com.google.cloud.bigquery.connector.common.AccessTokenProvider;\n"
          + "\n"
          + "public class HiddenAccessTokenProvider implements AccessTokenProvider {\n"
          + "  @Override\n"
          + "  public AccessToken getAccessToken() {\n"
          + "    return new AccessToken(\"hidden-token\", null);\n"
          + "  }\n"
          + "}\n";

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private ClassLoader originalContextClassLoader;

  @Before
  public void saveContextClassLoader() {
    originalContextClassLoader = Thread.currentThread().getContextClassLoader();
  }

  @After
  public void restoreContextClassLoader() {
    Thread.currentThread().setContextClassLoader(originalContextClassLoader);
  }

  @Test
  public void testLoadClassUsesContextClassLoader() throws Exception {
    ClassLoader hiddenClassLoader = createHiddenClassLoader();
    Thread.currentThread().setContextClassLoader(hiddenClassLoader);

    Class<?> clazz = BigQueryUtil.loadClass(HIDDEN_PROVIDER_CLASS_NAME);

    assertThat(clazz.getName()).isEqualTo(HIDDEN_PROVIDER_CLASS_NAME);
    assertThat(clazz.getClassLoader()).isSameInstanceAs(hiddenClassLoader);
  }

  @Test
  public void testLoadClassFallsBackWhenContextClassLoaderCannotFindClass() throws Exception {
    // A class loader that only sees the bootstrap classes, not the connector's.
    Thread.currentThread().setContextClassLoader(new URLClassLoader(new URL[0], null));

    Class<?> clazz = BigQueryUtil.loadClass(FileCredentialsAccessTokenProvider.class.getName());

    assertThat(clazz).isEqualTo(FileCredentialsAccessTokenProvider.class);
  }

  @Test
  public void testLoadClassFallsBackWhenContextClassLoaderIsNull() throws Exception {
    Thread.currentThread().setContextClassLoader(null);

    Class<?> clazz = BigQueryUtil.loadClass(FileCredentialsAccessTokenProvider.class.getName());

    assertThat(clazz).isEqualTo(FileCredentialsAccessTokenProvider.class);
  }

  @Test
  public void testLoadClassNotFound() {
    assertThrows(ClassNotFoundException.class, () -> BigQueryUtil.loadClass("no.such.Class"));
  }

  @Test
  public void testCreateVerifiedInstanceWithClassFromContextClassLoader() throws Exception {
    Thread.currentThread().setContextClassLoader(createHiddenClassLoader());

    AccessTokenProvider provider =
        BigQueryUtil.createVerifiedInstance(HIDDEN_PROVIDER_CLASS_NAME, AccessTokenProvider.class);

    assertThat(provider.getAccessToken().getTokenValue()).isEqualTo("hidden-token");
  }

  @Test
  public void testVerifySerializationWithClassFromContextClassLoader() throws Exception {
    ClassLoader hiddenClassLoader = createHiddenClassLoader();
    Thread.currentThread().setContextClassLoader(hiddenClassLoader);
    AccessTokenProvider provider = newHiddenProvider(hiddenClassLoader);

    AccessTokenProvider copy = BigQueryUtil.verifySerialization(provider);

    assertThat(copy.getClass()).isSameInstanceAs(provider.getClass());
    assertThat(copy.getAccessToken().getTokenValue()).isEqualTo("hidden-token");
  }

  @Test
  public void testCredentialsSerializationWithClassFromContextClassLoader() throws Exception {
    ClassLoader hiddenClassLoader = createHiddenClassLoader();
    Thread.currentThread().setContextClassLoader(hiddenClassLoader);
    AccessTokenProvider provider = newHiddenProvider(hiddenClassLoader);

    Credentials copy =
        BigQueryUtil.getCredentialsFromByteArray(
            BigQueryUtil.getCredentialsByteArray(new AccessTokenProviderCredentials(provider)));

    assertThat(copy).isInstanceOf(AccessTokenProviderCredentials.class);
    assertThat(((AccessTokenProviderCredentials) copy).getAccessTokenProvider().getClass())
        .isSameInstanceAs(provider.getClass());
  }

  private static AccessTokenProvider newHiddenProvider(ClassLoader hiddenClassLoader)
      throws Exception {
    return (AccessTokenProvider)
        hiddenClassLoader.loadClass(HIDDEN_PROVIDER_CLASS_NAME).getConstructor().newInstance();
  }

  private ClassLoader createHiddenClassLoader() throws Exception {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    assumeTrue("Compiling the test class requires a JDK", compiler != null);

    Path sourceFile =
        temporaryFolder
            .newFolder("src")
            .toPath()
            .resolve(HIDDEN_PROVIDER_CLASS_NAME.replace('.', '/') + ".java");
    Files.createDirectories(sourceFile.getParent());
    Files.write(sourceFile, HIDDEN_PROVIDER_SOURCE.getBytes(UTF_8));
    File classesDir = temporaryFolder.newFolder("classes");
    String classpath =
        classpathEntryOf(AccessTokenProvider.class)
            + File.pathSeparator
            + classpathEntryOf(com.google.auth.oauth2.AccessToken.class);

    int result =
        compiler.run(
            null,
            null,
            null,
            "-classpath",
            classpath,
            "-d",
            classesDir.getPath(),
            sourceFile.toString());

    assertThat(result).isEqualTo(0);
    return new URLClassLoader(
        new URL[] {classesDir.toURI().toURL()},
        BigQueryUtilClassLoadingTest.class.getClassLoader());
  }

  private static String classpathEntryOf(Class<?> clazz) throws Exception {
    return Paths.get(clazz.getProtectionDomain().getCodeSource().getLocation().toURI()).toString();
  }
}
