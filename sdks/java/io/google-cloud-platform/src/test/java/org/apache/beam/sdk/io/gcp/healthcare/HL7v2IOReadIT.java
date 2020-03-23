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

import com.google.api.services.healthcare.v1alpha2.model.Message;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.datastore.V1TestOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.values.PCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class HL7v2IOReadIT {
  private HL7v2IOTestOptions options;
  private String project;
  private final long numMessages = 1000;
  private transient HealthcareApiClient client;

  @Before
  public void setup() throws Exception {
    if (client == null) {
      client = new HttpHealthcareApiClient();
    }
    PipelineOptionsFactory.register(V1TestOptions.class);
    options = TestPipeline.testingPipelineOptions().as(HL7v2IOTestOptions.class);
    // Create HL7 messages and write them to HL7v2 Store.
    writeHL7v2Messages(options, numMessages);
  }

  @After
  public void tearDown() throws Exception {
    deleteAllHL7v2Messages(options);
  }

  @Test
  public void testHL7v2IORead() throws Exception {
    // Should read all messages.
    Pipeline pipeline = Pipeline.create(options);
    HL7v2IO.Read.Result result =
        pipeline
            .apply(
                new HL7v2IO.ListHL7v2MessageIDs(Collections.singletonList(options.getHl7v2Store())))
            .apply(HL7v2IO.readAll());
    PCollection<Long> numReadMessages = result.getMessages().apply(Count.globally());
    PAssert.thatSingleton(numReadMessages).isEqualTo(numMessages);
  }

  @Test
  public void testHL7v2IOFilteredRead() throws Exception {
    final String filter = "messageType = \"ADT\"";
    final long numADT = 10; // TODO replace this placeholder once we have test data.
    // Should read only messages matching the filter.
    Pipeline pipeline = Pipeline.create(options);
    HL7v2IO.Read.Result result =
        pipeline
            .apply(
                new HL7v2IO.ListHL7v2MessageIDs(
                    Collections.singletonList(options.getHl7v2Store()), filter))
            .apply(HL7v2IO.readAll());
    PCollection<Long> numReadMessages = result.getMessages().apply(Count.globally());
    PAssert.thatSingleton(numReadMessages).isEqualTo(numADT);
  }

  /** Clear all messages from the HL7v2 store. */
  private void deleteAllHL7v2Messages(HL7v2IOTestOptions options) throws IOException {
    List<IOException> deleteErrors = new ArrayList<>();
    client
        .getHL7v2MessageIDStream(options.getHl7v2Store())
        .forEach(
            (String msgID) -> {
              try {
                client.deleteHL7v2Message(msgID);
              } catch (IOException e) {
                e.printStackTrace();
                deleteErrors.add(e);
              }
            });

    if (deleteErrors.size() > 0) {
      throw deleteErrors.get(0);
    }
  }

  /** Populate the test messages into the HL7v2 store. */
  private void writeHL7v2Messages(HL7v2IOTestOptions options, long numMessages) throws IOException {
    Message msg = new Message();

    for (int i = 0; i < numMessages; i++) {
      // TODO modify each new message or read from resource file.
      client.createHL7v2Message(options.getHl7v2Store(), msg);
    }
  }
}
