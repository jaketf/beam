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

import static org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.MESSAGES;
import static org.apache.beam.sdk.io.gcp.healthcare.HL7v2IOTestUtil.deleteAllHL7v2Messages;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HL7v2IOWriteIT {

  private HL7v2IOTestOptions options;
  private transient HealthcareApiClient client;

  @Before
  public void setup() throws Exception {
    if (client == null) {
      client = new HttpHealthcareApiClient();
    }
    PipelineOptionsFactory.register(HL7v2IOTestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HL7v2IOTestOptions.class);
    options.setHL7v2Store(
        "projects/jferriero-dev/locations/us-central1/datasets/raw-dataset/hl7V2Stores/jake-hl7");
  }

  @After
  public void tearDown() throws Exception {
    deleteAllHL7v2Messages(client, options.getHL7v2Store());
  }

  @Test
  public void testHL7v2IOWrite() throws IOException {
    Pipeline pipeline = Pipeline.create(options);
    HL7v2IO.Write.Result result =
        pipeline
            .apply(Create.of(MESSAGES).withCoder(new HL7v2MessageCoder()))
            .apply(HL7v2IO.ingestMessages(options.getHL7v2Store()));

    PAssert.that(result.getFailedInsertsWithErr()).empty();

    pipeline.run().waitUntilFinish();
    long numWrittenMessages = client.getHL7v2MessageStream(options.getHL7v2Store()).count();
    assertEquals(MESSAGES.size(), numWrittenMessages);
  }
}