/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.gcp.healthcare;

import static org.apache.beam.sdk.io.gcp.healthcare.FhirIOTestUtil.PRETTY_BUNDLES;
import static org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.HEALTHCARE_DATASET_TEMPLATE;

import com.google.api.services.healthcare.v1beta1.model.HttpBody;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.UUID;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.healthcare.FhirIO.Import.ContentStructure;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FhirIOWriteIT {

  public static FhirIOTestOptions options;
  private static FhirIOTestUtil util;
  private static String project;
  private transient HealthcareApiClient client;
  private static String healthcareDataset;
  private static final String FHIR_STORE_NAME =
      "fhir_store_"
          + System.currentTimeMillis()
          + "_"
          + (new SecureRandom().nextInt(32))
          + "_write_it";

  private static final String TEMP_BUCKET =
      "fhir-store-import-it"
          + UUID.randomUUID().toString();


  @BeforeClass
  public static void setUp() throws Exception {
    project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    project = "jferriero-dev"; // TODO(jaketf) DELETEME
    PipelineOptionsFactory.register(FhirIOTestOptions.class);
    if (util == null) {
      util = new FhirIOTestUtil();
    }
    util.createBucket(project, TEMP_BUCKET);
    options = TestPipeline.testingPipelineOptions().as(FhirIOTestOptions.class);
    options.setGcsTempPath(String.format("gs://%s/FhirIOWriteIT/temp/", TEMP_BUCKET));
    options.setGcsDeadLetterPath(String.format("gs://%s/FhirIOWriteIT/deadletter/", TEMP_BUCKET));
    options.setFhirStore(healthcareDataset + "/fhirStores/" + FHIR_STORE_NAME);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.deleteBucket(TEMP_BUCKET);
  }

  /** Create / Destroy FHIR store on every test because deleteing all resources between tests
   * is not a straightforward operation, simpler to just delete the whole store. */
  @Before
  public void createFhirStore() throws IOException {
    project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    project = "jferriero-dev"; // TODO(jaketf) DELETEME
    healthcareDataset = String.format(HEALTHCARE_DATASET_TEMPLATE, project);
    HealthcareApiClient client = new HttpHealthcareApiClient();
    client.createFhirStore(healthcareDataset, FHIR_STORE_NAME);
  }

  @After
  public void deleteFhirStore() throws IOException {
    HealthcareApiClient client = new HttpHealthcareApiClient();
    client.deleteFhirStore(healthcareDataset + "/fhirStores/" + FHIR_STORE_NAME);
  }


  @Test
  public void testFhirIO_ExecuteBundle() throws IOException {
    Pipeline pipeline = Pipeline.create(options);
    FhirIO.Write.Result writeResult =
        pipeline
            .apply(Create.of(PRETTY_BUNDLES).withCoder(new HttpBodyCoder()))
            .apply(FhirIO.Write.executeBundles(options.getFhirStore()));

    writeResult.getFailedInsertsWithErr()
        .apply(MapElements.into(TypeDescriptors.strings()).via(
            (HealthcareIOError<HttpBody> err) -> String.format(
                "Data: %s \n"
                    + "Error: %s \n"
                    + "Stacktrace: %s", err.getDataResource().getData(), err.getErrorMessage(),
                err.getStackTrace())))
        .apply(TextIO.write().to("gs://jferriero-dev/FhirIOWriteDebugging/errors"));

    PAssert.that(writeResult.getFailedInsertsWithErr()).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testFhirIO_Import() throws IOException {
    Pipeline pipeline = Pipeline.create(options);
    FhirIO.Write.Result result =
        pipeline
            .apply(Create.of(PRETTY_BUNDLES).withCoder(new HttpBodyCoder()))
            .apply(
                FhirIO.Write.fhirStoresImport(
                    options.getFhirStore(),
                    options.getGcsTempPath(),
                    options.getGcsDeadLetterPath(),
                    ContentStructure.BUNDLE));

    result.getFailedInsertsWithErr()
        .apply(MapElements.into(TypeDescriptors.strings()).via(
            (HealthcareIOError<HttpBody> err) -> String.format(
                "Data: %s \n"
                    + "Error: %s \n"
                    + "Stacktrace: %s", err.getDataResource().getData(), err.getErrorMessage(),
                err.getStackTrace())))
        .apply(TextIO.write().to("gs://jferriero-dev/FhirIOWriteDebugging/errors"));

    PAssert.that(result.getFailedInsertsWithErr()).empty();

    pipeline.run().waitUntilFinish();
  }
}
