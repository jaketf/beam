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

import static org.apache.beam.sdk.io.gcp.healthcare.FhirIOTestUtil.R4_PRETTY_BUNDLES;
import static org.apache.beam.sdk.io.gcp.healthcare.FhirIOTestUtil.TEMP_BUCKET;
import static org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.HEALTHCARE_DATASET_TEMPLATE;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.healthcare.v1beta1.model.HttpBody;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.net.URI;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class FhirIOR4WriteIT {

  private FhirIOTestOptions options;
  private transient HealthcareApiClient client;
  private static String healthcareDataset;
  private static long testTime = System.currentTimeMillis();
  private static final String FHIR_STORE_NAME =
      "FHIR_store_r4_write_it_" + testTime + "_" + (new SecureRandom().nextInt(32));

  @Before
  public void setup() throws Exception {
    if (client == null) {
      client = new HttpHealthcareApiClient();
    }
    GcpOptions gcpOptions = TestPipeline.testingPipelineOptions().as(GcpOptions.class);
    PipelineOptionsFactory.register(FhirIOTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(FhirIOTestOptions.class);
    options.setGcsTempPath(
        String.format("gs://%s/FhirIOR4WriteIT/%s/temp/", TEMP_BUCKET, testTime));
    options.setGcsDeadLetterPath(
        String.format("gs://%s/FhirIOR4WriteIT/%s/deadletter/", TEMP_BUCKET, testTime));
    options.setFhirStore(healthcareDataset + "/fhirStores/" + FHIR_STORE_NAME);
  }

  @BeforeClass
  public static void setupEnvironment() throws IOException {
    String project = TestPipeline.testingPipelineOptions().as(GcpOptions.class).getProject();
    healthcareDataset = String.format(HEALTHCARE_DATASET_TEMPLATE, project);
    HealthcareApiClient client = new HttpHealthcareApiClient();
    client.createFhirStore(healthcareDataset, FHIR_STORE_NAME, "R4");
  }

  @AfterClass
  public static void teardownEnvironment() throws IOException {
    HealthcareApiClient client = new HttpHealthcareApiClient();
    client.deleteFhirStore(healthcareDataset + "/fhirStores/" + FHIR_STORE_NAME);
    // clean up GCS objects if any.
    FhirIOTestUtil.tearDownTempBucket();
  }

  @Test
  public void testFhirIO_ExecuteBundle() throws IOException {
    Pipeline pipeline = Pipeline.create(options);
    FhirIO.Write.Result writeResult =
        pipeline
            .apply(Create.of(R4_PRETTY_BUNDLES).withCoder(new HttpBodyCoder()))
            .apply(FhirIO.Write.executeBundles(options.getFhirStore()));

    PAssert.that(writeResult.getFailedInsertsWithErr()).empty();

    pipeline.run().waitUntilFinish();
  }

  @Test
  public void testFhirIO_Import() throws IOException {
    Pipeline pipeline = Pipeline.create(options);
    FhirIO.Write.Result result =
        pipeline
            .apply(Create.of(R4_PRETTY_BUNDLES).withCoder(new HttpBodyCoder()))
            .apply(
                FhirIO.Write.fhirStoresImport(
                    options.getFhirStore(),
                    options.getGcsTempPath(),
                    options.getGcsDeadLetterPath(),
                    ContentStructure.BUNDLE));

    PAssert.that(result.getFailedInsertsWithErr()).empty();

    pipeline.run().waitUntilFinish();
  }
}
