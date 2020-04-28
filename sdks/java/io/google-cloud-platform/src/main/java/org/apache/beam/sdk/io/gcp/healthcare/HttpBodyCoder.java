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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.api.services.healthcare.v1beta1.model.HttpBody;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StringUtf8Coder;

public class HttpBodyCoder extends AtomicCoder<HttpBody> {

  public static HttpBodyCoder of() {
    return INSTANCE;
  }

  private static final Coder<String> STRING_CODER = StringUtf8Coder.of();
  private static final ObjectMapper MAPPER =
      new ObjectMapper().disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

  public HttpBodyCoder() {}

  @Override
  public void encode(HttpBody value, OutputStream outStream) throws IOException {
    String jsonStr = MAPPER.writeValueAsString(value);
    STRING_CODER.encode(jsonStr, outStream);
  }

  @Override
  public HttpBody decode(InputStream inStream) throws IOException {
    String strValue = StringUtf8Coder.of().decode(inStream);
    return MAPPER.readValue(strValue, HttpBody.class);
  }

  private static final HttpBodyCoder INSTANCE = new HttpBodyCoder();
}
