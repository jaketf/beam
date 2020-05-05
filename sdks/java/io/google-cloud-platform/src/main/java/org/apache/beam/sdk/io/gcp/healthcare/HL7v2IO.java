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

import com.google.api.services.healthcare.v1beta1.model.Message;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.text.ParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.sdk.values.PValue;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Throwables;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link HL7v2IO} provides an API for reading from and writing to <a
 * href="https://cloud.google.com/healthcare/docs/concepts/hl7v2">Google Cloud Healthcare HL7v2 API.
 * </a>
 *
 * <p>Read
 *
 * <p>HL7v2 Messages can be fetched from the HL7v2 store in two ways Message Fetching and Message
 * Listing.
 *
 * <p>Message Fetching
 *
 * <p>Message Fetching with {@link HL7v2IO.Read} supports use cases where you have a ${@link
 * PCollection} of message IDS. This is appropriate for reading the HL7v2 notifications from a
 * Pub/Sub subscription with {@link PubsubIO#readStrings()} or in cases where you have a manually
 * prepared list of messages that you need to process (e.g. in a text file read with {@link
 * org.apache.beam.sdk.io.TextIO}) .
 *
 * <p>Fetch Message contents from HL7v2 Store based on the {@link PCollection} of message ID strings
 * {@link HL7v2IO.Read.Result} where one can call {@link Read.Result#getMessages()} to retrived a
 * {@link PCollection} containing the successfully fetched {@link HL7v2Message}s and/or {@link
 * Read.Result#getFailedReads()} to retrieve a {@link PCollection} of {@link HealthcareIOError}
 * containing the msgID that could not be fetched and the exception as a {@link HealthcareIOError},
 * this can be used to write to the dead letter storage system of your choosing. This error handling
 * is mainly to catch scenarios where the upstream {@link PCollection} contains IDs that are not
 * valid or are not reachable due to permissions issues.
 *
 * <p>Message Listing Message Listing with {@link HL7v2IO.ListHL7v2Messages} supports batch use
 * cases where you want to process all the messages in an HL7v2 store or those matching a
 * filter @see <a
 * href=>https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.hl7V2Stores.messages/list#query-parameters</a>
 * This paginates through results of a Messages.List call @see <a
 * href=>https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.hl7V2Stores.messages/list</a>
 * and outputs directly to a {@link PCollection} of {@link HL7v2Message}. In these use cases, the
 * error handling similar to above is unnecessary because we are listing from the source of truth
 * the pipeline should fail transparently if this transform fails to paginate through all the
 * results.
 *
 * <p>Write
 *
 * <p>A bounded or unbounded {@link PCollection} of {@link HL7v2Message} can be ingested into an
 * HL7v2 store using {@link HL7v2IO#ingestMessages(String)}. This will return a {@link
 * HL7v2IO.Write.Result} on which you can call {@link Write.Result#getFailedInsertsWithErr()} to
 * retrieve a {@link PCollection} of {@link HealthcareIOError} containing the {@link HL7v2Message}
 * that failed to be ingested and the exception. This can be used to write to the dead letter
 * storage system of your chosing.
 *
 * <p>Unbounded Read Example:
 *
 * <pre>{@code
 * PipelineOptions options = ...;
 * Pipeline p = Pipeline.create(options);
 *
 * HL7v2IO.Read.Result readResult = p
 *   .apply(
 *     "Read HL7v2 notifications",
 *     PubsubIO.readStrings().fromSubscription(options.getNotificationSubscription()))
 *   .apply(HL7v2IO.getAll());
 *
 * // Write errors to your favorite dead letter  queue (e.g. Pub/Sub, GCS, BigQuery)
 * readResult.getFailedReads().apply("WriteToDeadLetterQueue", ...);
 *
 *
 * // Go about your happy path transformations.
 * PCollection<HL7v2Message> out = readResult.getMessages().apply("ProcessFetchedMessages", ...);
 *
 * // Write using the Message.Ingest method of the HL7v2 REST API.
 * out.apply(HL7v2IO.ingestMessages(options.getOutputHL7v2Store()));
 *
 * pipeline.run();
 *
 * }***
 * </pre>
 *
 * <p>Bounded Read Example:
 *
 * <pre>{@code
 * PipelineOptions options = ...;
 * Pipeline p = Pipeline.create(options);
 *
 * PCollection<HL7v2Message> out = p
 *   .apply(
 *       "List messages in HL7v2 store with filter",
 *       ListHL7v2Messages(
 *           Collections.singletonList(options.getInputHL7v2Store()), option.getHL7v2Filter()))
 *    // Go about your happy path transformations.
 *   .apply("Process HL7v2 Messages", ...);
 * pipeline.run().waitUntilFinish();
 * }***
 * </pre>
 */
public class HL7v2IO {

  /** Write HL7v2 Messages to a store. */
  private static Write.Builder write(String hl7v2Store) {
    return new AutoValue_HL7v2IO_Write.Builder().setHL7v2Store(hl7v2Store);
  }

  /** Write HL7v2 Messages to a store. */
  private static Write.Builder write(ValueProvider<String> hl7v2Store) {
    return new AutoValue_HL7v2IO_Write.Builder().setHL7v2Store(hl7v2Store.get());
  }

  /**
   * Retrieve all HL7v2 Messages from a PCollection of message IDs (such as from PubSub notification
   * subscription).
   */
  public static Read getAll() {
    return new Read();
  }

  /** Read all HL7v2 Messages from multiple stores. */
  public static ListHL7v2Messages readAll(List<String> hl7v2Stores) {
    return new ListHL7v2Messages(StaticValueProvider.of(hl7v2Stores), StaticValueProvider.of(null));
  }

  /** Read all HL7v2 Messages from multiple stores. */
  public static ListHL7v2Messages readAll(ValueProvider<List<String>> hl7v2Stores) {
    return new ListHL7v2Messages(hl7v2Stores, StaticValueProvider.of(null));
  }

  /** Read all HL7v2 Messages from a single store. */
  public static ListHL7v2Messages read(String hl7v2Store) {
    return new ListHL7v2Messages(
        StaticValueProvider.of(Collections.singletonList(hl7v2Store)),
        StaticValueProvider.of(null));
  }

  /** Read all HL7v2 Messages from a single store. */
  public static ListHL7v2Messages read(ValueProvider<String> hl7v2Store) {
    return new ListHL7v2Messages(
        StaticValueProvider.of(Collections.singletonList(hl7v2Store.get())),
        StaticValueProvider.of(null));
  }

  /**
   * Read all HL7v2 Messages from a single store matching a filter.
   *
   * @see <a
   *     href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.hl7V2Stores.messages/list#query-parameters></a>
   */
  public static ListHL7v2Messages readWithFilter(String hl7v2Store, String filter) {
    return new ListHL7v2Messages(
        StaticValueProvider.of(Collections.singletonList(hl7v2Store)),
        StaticValueProvider.of(filter));
  }

  /**
   * Read all HL7v2 Messages from a single store matching a filter.
   *
   * @see <a
   *     href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.hl7V2Stores.messages/list#query-parameters></a>
   */
  public static ListHL7v2Messages readWithFilter(
      ValueProvider<String> hl7v2Store, ValueProvider<String> filter) {
    return new ListHL7v2Messages(
        StaticValueProvider.of(Collections.singletonList(hl7v2Store.get())), filter);
  }

  /**
   * Read all HL7v2 Messages from a multiple stores matching a filter.
   *
   * @see <a
   *     href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.hl7V2Stores.messages/list#query-parameters></a>
   */
  public static ListHL7v2Messages readAllWithFilter(List<String> hl7v2Stores, String filter) {
    return new ListHL7v2Messages(
        StaticValueProvider.of(hl7v2Stores), StaticValueProvider.of(filter));
  }

  /**
   * Read all HL7v2 Messages from a multiple stores matching a filter.
   *
   * @see <a
   *     href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.hl7V2Stores.messages/list#query-parameters></a>
   */
  public static ListHL7v2Messages readAllWithFilter(
      ValueProvider<List<String>> hl7v2Stores, ValueProvider<String> filter) {
    return new ListHL7v2Messages(hl7v2Stores, filter);
  }

  /**
   * Write with Messages.Ingest method. @see <a
   * href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.hl7V2Stores.messages/ingest></a>
   *
   * @param hl7v2Store the hl 7 v 2 store
   * @return the write
   */
  public static Write ingestMessages(String hl7v2Store) {
    return write(hl7v2Store).setWriteMethod(Write.WriteMethod.INGEST).build();
  }

  /**
   * The type Read that reads HL7v2 message contents given a PCollection of message IDs strings.
   *
   * <p>These could be sourced from any {@link PCollection} of {@link String}s but the most popular
   * patterns would be {@link PubsubIO#readStrings()} reading a subscription on an HL7v2 Store's
   * notification channel topic or using {@link ListHL7v2Messages} to list HL7v2 message IDs with an
   * optional filter using Ingest write method. @see <a
   * href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.hl7V2Stores.messages/list></a>.
   */
  public static class Read extends PTransform<PCollection<String>, Read.Result> {

    public Read() {}

    public static class Result implements POutput, PInput {
      private PCollection<HL7v2Message> messages;

      private PCollection<HealthcareIOError<String>> failedReads;
      PCollectionTuple pct;

      public static Result of(PCollectionTuple pct) throws IllegalArgumentException {
        if (pct.getAll()
            .keySet()
            .containsAll((Collection<?>) TupleTagList.of(OUT).and(DEAD_LETTER))) {
          return new Result(pct);
        } else {
          throw new IllegalArgumentException(
              "The PCollection tuple must have the HL7v2IO.Read.OUT "
                  + "and HL7v2IO.Read.DEAD_LETTER tuple tags");
        }
      }

      private Result(PCollectionTuple pct) {
        this.pct = pct;
        this.messages = pct.get(OUT).setCoder(new HL7v2MessageCoder());
        this.failedReads =
            pct.get(DEAD_LETTER).setCoder(new HealthcareIOErrorCoder<>(StringUtf8Coder.of()));
      }

      public PCollection<HealthcareIOError<String>> getFailedReads() {
        return failedReads;
      }

      public PCollection<HL7v2Message> getMessages() {
        return messages;
      }

      @Override
      public Pipeline getPipeline() {
        return this.pct.getPipeline();
      }

      @Override
      public Map<TupleTag<?>, PValue> expand() {
        return ImmutableMap.of(OUT, messages);
      }

      @Override
      public void finishSpecifyingOutput(
          String transformName, PInput input, PTransform<?, ?> transform) {}
    }

    /** The tag for the main output of HL7v2 Messages. */
    public static final TupleTag<HL7v2Message> OUT = new TupleTag<HL7v2Message>() {};
    /** The tag for the deadletter output of HL7v2 Messages. */
    public static final TupleTag<HealthcareIOError<String>> DEAD_LETTER =
        new TupleTag<HealthcareIOError<String>>() {};

    @Override
    public Result expand(PCollection<String> input) {
      return input.apply("Fetch HL7v2 messages", new FetchHL7v2Message());
    }

    /**
     * {@link PTransform} to fetch a message from an Google Cloud Healthcare HL7v2 store based on
     * msgID.
     *
     * <p>This DoFn consumes a {@link PCollection} of notifications {@link String}s from the HL7v2
     * store, and fetches the actual {@link HL7v2Message} object based on the id in the notification
     * and will output a {@link PCollectionTuple} which contains the output and dead-letter {@link
     * PCollection}.
     *
     * <p>The {@link PCollectionTuple} output will contain the following {@link PCollection}:
     *
     * <ul>
     *   <li>{@link HL7v2IO.Read#OUT} - Contains all {@link PCollection} records successfully read
     *       from the HL7v2 store.
     *   <li>{@link HL7v2IO.Read#DEAD_LETTER} - Contains all {@link PCollection} of {@link
     *       HealthcareIOError} message IDs which failed to be fetched from the HL7v2 store, with
     *       error message and stacktrace.
     * </ul>
     */
    public static class FetchHL7v2Message extends PTransform<PCollection<String>, Result> {

      /** Instantiates a new Fetch HL7v2 message DoFn. */
      public FetchHL7v2Message() {}

      @Override
      public Result expand(PCollection<String> msgIds) {
        return new Result(
            msgIds.apply(
                ParDo.of(new FetchHL7v2Message.HL7v2MessageGetFn())
                    .withOutputTags(HL7v2IO.Read.OUT, TupleTagList.of(HL7v2IO.Read.DEAD_LETTER))));
      }

      /** DoFn for fetching messages from the HL7v2 store with error handling. */
      public static class HL7v2MessageGetFn extends DoFn<String, HL7v2Message> {

        private Counter failedMessageGets =
            Metrics.counter(FetchHL7v2Message.HL7v2MessageGetFn.class, "failed-message-reads");
        private static final Logger LOG =
            LoggerFactory.getLogger(FetchHL7v2Message.HL7v2MessageGetFn.class);
        private final Counter successfulHL7v2MessageGets =
            Metrics.counter(
                FetchHL7v2Message.HL7v2MessageGetFn.class, "successful-hl7v2-message-gets");
        private HealthcareApiClient client;

        /** Instantiates a new Hl 7 v 2 message get fn. */
        HL7v2MessageGetFn() {}

        /**
         * Instantiate healthcare client.
         *
         * @throws IOException the io exception
         */
        @Setup
        public void instantiateHealthcareClient() throws IOException {
          this.client = new HttpHealthcareApiClient();
        }

        /**
         * Process element.
         *
         * @param context the context
         */
        @ProcessElement
        public void processElement(ProcessContext context) {
          String msgId = context.element();
          try {
            context.output(HL7v2Message.fromModel(fetchMessage(this.client, msgId)));
          } catch (Exception e) {
            failedMessageGets.inc();
            LOG.warn(
                String.format(
                    "Error fetching HL7v2 message with ID %s writing to Dead Letter "
                        + "Queue. Cause: %s Stack Trace: %s",
                    msgId, e.getMessage(), Throwables.getStackTraceAsString(e)));
            context.output(HL7v2IO.Read.DEAD_LETTER, HealthcareIOError.of(msgId, e));
          }
        }

        private Message fetchMessage(HealthcareApiClient client, String msgId)
            throws IOException, ParseException, IllegalArgumentException, InterruptedException {
          long startTime = System.currentTimeMillis();

          try {
            com.google.api.services.healthcare.v1beta1.model.Message msg =
                client.getHL7v2Message(msgId);
            if (msg == null) {
              throw new IOException(String.format("GET request for %s returned null", msgId));
            }
            this.successfulHL7v2MessageGets.inc();
            return msg;
          } catch (Exception e) {
            throw e;
          }
        }
      }
    }
  }

  /**
   * List HL7v2 messages in HL7v2 Stores with optional filter.
   *
   * <p>This transform is optimized for dynamic splitting of message.list calls for large batches of
   * historical data and assumes rather continuous stream of sendTimes. It will dynamically
   * rebalance resources to handle "peak traffic times" but will waste resources if there are large
   * durations (days) of the sendTime dimension without data.
   *
   * <p>Implementation includes overhead for: 1. two api calls to determine the min/max sendTime of
   * the HL7v2 store at invocation time. 2. initial splitting into non-overlapping time ranges
   * (default daily) to achieve parallelization in separate messages.list calls.
   *
   * <p>This will make more queries than necessary when used with very small data sets. (or very
   * sparse data sets in the sendTime dimension).
   *
   * <p>If you have large but sparse data (e.g. hours between consecutive message sendTimes) and
   * know something about the time ranges where you have no data, consider using multiple instances
   * of this transform specifying sendTime filters to omit the ranges where there is no data.
   */
  public static class ListHL7v2Messages extends PTransform<PBegin, PCollection<HL7v2Message>> {
    private final List<String> hl7v2Stores;
    private String filter;
    private Duration initialSplitDuration;

    /**
     * Instantiates a new List HL7v2 message IDs with filter.
     *
     * @param hl7v2Stores the HL7v2 stores
     * @param filter the filter
     */
    ListHL7v2Messages(ValueProvider<List<String>> hl7v2Stores, ValueProvider<String> filter) {
      this.hl7v2Stores = hl7v2Stores.get();
      this.filter = filter.get();
    }

    /**
     * Instantiates a new List hl 7 v 2 messages.
     *
     * @param hl7v2Stores the hl 7 v 2 stores
     * @param filter the filter
     * @param initialSplitDuration the initial split duration for sendTime dimension splits
     */
    ListHL7v2Messages(
        ValueProvider<List<String>> hl7v2Stores,
        ValueProvider<String> filter,
        Duration initialSplitDuration) {
      this.hl7v2Stores = hl7v2Stores.get();
      this.filter = filter.get();
      this.initialSplitDuration = initialSplitDuration;
    }

    /**
     * Instantiates a new List hl7v2 messages.
     *
     * @param hl7v2Stores the hl7v2 stores
     */
    ListHL7v2Messages(ValueProvider<List<String>> hl7v2Stores) {
      this.hl7v2Stores = hl7v2Stores.get();
      this.filter = null;
    }

    /**
     * Instantiates a new List hl7v2 messages.
     *
     * @param hl7v2Stores the hl7v2 stores
     * @param initialSplitDuration the initial split duration
     */
    ListHL7v2Messages(
        ValueProvider<List<String>> hl7v2Stores,
        Duration initialSplitDuration) {
      this.hl7v2Stores = hl7v2Stores.get();
      this.initialSplitDuration = initialSplitDuration;
    }

    @Override
    public PCollection<HL7v2Message> expand(PBegin input) {
      return input
          .apply(Create.of(this.hl7v2Stores))
          .apply(ParDo.of(new ListHL7v2MessagesFn(this.filter, initialSplitDuration)))
          .setCoder(new HL7v2MessageCoder())
          // Break fusion to encourage parallelization of downstream processing.
          .apply(Reshuffle.viaRandomKey());
    }
  }

  /**
   * Implemented as Splitable DoFn that claims millisecond resolutions of offset restrictions in the
   * Message.sendTime dimension.
   */
  @BoundedPerElement
  static class ListHL7v2MessagesFn extends DoFn<String, HL7v2Message> {

    private static final Logger LOG = LoggerFactory.getLogger(ListHL7v2MessagesFn.class);
    private String filter;
    // TODO(jaketf) what are reasonable defaults here?
    // These control the initial restriction split which means that the list of integer pairs
    // must comfortably fit in memory.
    private static final Duration DEFAULT_DESIRED_SPLIT_DURATION = Duration.standardDays(1);
    private static final Duration DEFAULT_MIN_SPLIT_DURATION = Duration.standardHours(1);
    private Duration initialSplitDuration;
    private Instant from;
    private Instant to;
    private transient HealthcareApiClient client;
    private Distribution messageListingLatencyMs =
        Metrics.distribution(ListHL7v2MessagesFn.class, "message-list-pagination-latency-ms");
    /**
     * Instantiates a new List HL7v2 fn.
     *
     * @param filter the filter
     */
    ListHL7v2MessagesFn(String filter) {
      new ListHL7v2MessagesFn(filter, null);
    }

    ListHL7v2MessagesFn(String filter, @Nullable Duration initialSplitDuration) {
      this.filter = filter;
      this.initialSplitDuration =
          (initialSplitDuration == null) ? DEFAULT_DESIRED_SPLIT_DURATION : initialSplitDuration;
    }

    /**
     * Init client.
     *
     * @throws IOException the io exception
     */
    @Setup
    public void initClient() throws IOException {
      this.client = new HttpHealthcareApiClient();
    }

    @GetInitialRestriction
    public OffsetRange getEarliestToNowRestriction(@Element String hl7v2Store) throws IOException {
      from = this.client.getEarliestHL7v2SendTime(hl7v2Store, this.filter);
      // filters are [from, to) to match logic of OffsetRangeTracker but need latest element to be
      // included in results set to add an extra ms to the upper bound.
      to = this.client.getLatestHL7v2SendTime(hl7v2Store, this.filter).plus(1);
      return new OffsetRange(from.getMillis(), to.getMillis());
    }

    @NewTracker
    public OffsetRangeTracker newTracker(@Restriction OffsetRange timeRange) {
      return timeRange.newTracker();
    }

    @SplitRestriction
    public void split(@Restriction OffsetRange timeRange, OutputReceiver<OffsetRange> out) {
      // TODO(jaketf) How to pick optimal values for desiredNumOffsetsPerSplit ?
      List<OffsetRange> splits =
          timeRange.split(initialSplitDuration.getMillis(), DEFAULT_MIN_SPLIT_DURATION.getMillis());
      Instant from = Instant.ofEpochMilli(timeRange.getFrom());
      Instant to = Instant.ofEpochMilli(timeRange.getTo());
      Duration totalDuration = new Duration(from, to);
      LOG.info(
          String.format(
              "splitting initial sendTime restriction of [minSendTime, now): [%s,%s), "
                  + "or [%s, %s). \n"
                  + "total days: %s \n"
                  + "into %s splits. \n"
                  + "Last split: %s",
              from,
              to,
              timeRange.getFrom(),
              timeRange.getTo(),
              totalDuration.getStandardDays(),
              splits.size(),
              splits.get(splits.size() - 1).toString()));

      for (OffsetRange s : splits) {
        out.output(s);
      }
    }

    /**
     * List messages.
     *
     * @param hl7v2Store the HL7v2 store to list messages from
     * @throws IOException the io exception
     */
    @ProcessElement
    public void listMessages(
        @Element String hl7v2Store,
        RestrictionTracker tracker,
        OutputReceiver<HL7v2Message> outputReceiver)
        throws IOException {
      OffsetRange currentRestriction = (OffsetRange) tracker.currentRestriction();
      Instant startRestriction = Instant.ofEpochMilli(currentRestriction.getFrom());
      Instant endRestriction = Instant.ofEpochMilli(currentRestriction.getTo());
      HttpHealthcareApiClient.HL7v2MessagePages pages =
          new HttpHealthcareApiClient.HL7v2MessagePages(
              client, hl7v2Store, startRestriction, endRestriction, filter, "sendTime");
      long reqestTime = Instant.now().getMillis();
      // TODO(jaketf) this code is hard to follow. Break this up into unit testable pieces.
      // [Start] Hard to read, hard to test, hard to maintain code.
      long lastClaimedMilliSecond = currentRestriction.getFrom();
      Instant cursor = Instant.ofEpochMilli(currentRestriction.getFrom());
      boolean hangingClaim = false; // flag if the claimed ms spans spills over to the next page.
      for (List<HL7v2Message> page : pages) { // loop over pages.
        int i = 0;
        HL7v2Message msg = page.get(i);
        while (i < page.size()) { // loop over messages in page
          cursor = Instant.parse(msg.getSendTime());
          if (!hangingClaim) {
            // claim all outstanding ms up until this message.
            for (long j = lastClaimedMilliSecond + 1; j < cursor.getMillis(); j++) {
              if (!tracker.tryClaim(j)) {
                break;
              }
            }
          }
          lastClaimedMilliSecond = cursor.getMillis();
          LOG.info(
              String.format(
                  "initial claim for page %s lastClaimedMilliSecond = %s",
                  i, lastClaimedMilliSecond));
          if (hangingClaim || tracker.tryClaim(lastClaimedMilliSecond)) {
            // This means we have claimed an entire millisecond we need to make sure that we
            // process all messages for this millisecond because sendTime is nano second resolution.
            // https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.hl7V2Stores.messages#Message
            while (cursor.getMillis() == lastClaimedMilliSecond
                && i < page.size()) { // loop over messages in millisecond.
              outputReceiver.output(msg);
              msg = page.get(i++);
              cursor = Instant.parse(msg.getSendTime());
            }

            if (i == page.size() && cursor.getMillis() == lastClaimedMilliSecond) {
              // reached the end of the page and timestamp still in the claimed ms.
              hangingClaim = true;
              continue;
            }

            // If reached this point, msg.sendTime is outside the current claim.
            // Need to claim all milliseconds until cursor to properly advance the tracker.
            // TODO(jaketf): claiming every ms between messages in these for loops is very
            // inefficient! consider claiming larger intervals or implementing a different kind of
            // tracker.
            for (long j = lastClaimedMilliSecond + 1;
                j <= cursor.getMillis();
                j++) { // loop over and claim milliseconds between messages.
              if (!tracker.tryClaim(j)) {
                break;
              }
              lastClaimedMilliSecond++;
            }
            LOG.info(
                String.format(
                    "After claiming between messages lastClaimedMilliSecond = %s",
                    lastClaimedMilliSecond));
          }
        }
        messageListingLatencyMs.update(Instant.now().getMillis() - reqestTime);
        reqestTime =
            Instant.now()
                .getMillis(); // this is for updating listing latency metric in next iteration.
      }

      if (lastClaimedMilliSecond == currentRestriction.getFrom()) {
        // no messages found for this time interval need to claim first ms in the restriction
        // before claiming the rest in the loop below.
        if (!tracker.tryClaim(lastClaimedMilliSecond)) {
          return;
        }
      }

      // loop over and claim milliseconds between last claimed ms and end of OffsetRange to it is
      // completely claimed.
      for (long j = lastClaimedMilliSecond + 1; j <= currentRestriction.getTo(); j++) {
        if (!tracker.tryClaim(j)) {
          break;
        }
      }
      // [End] Hard to read, hard to test, hard to maintain code
    }
  }

  /** The type Write. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<HL7v2Message>, Write.Result> {

    /** The tag for the successful writes to HL7v2 store`. */
    public static final TupleTag<HealthcareIOError<HL7v2Message>> SUCCESS =
        new TupleTag<HealthcareIOError<HL7v2Message>>() {};
    /** The tag for the failed writes to HL7v2 store`. */
    public static final TupleTag<HealthcareIOError<HL7v2Message>> FAILED =
        new TupleTag<HealthcareIOError<HL7v2Message>>() {};

    /**
     * Gets HL7v2 store.
     *
     * @return the HL7v2 store
     */
    abstract String getHL7v2Store();

    /**
     * Gets write method.
     *
     * @return the write method
     */
    abstract WriteMethod getWriteMethod();

    @Override
    public Result expand(PCollection<HL7v2Message> messages) {
      return messages.apply(new WriteHL7v2(this.getHL7v2Store(), this.getWriteMethod()));
    }

    /** The enum Write method. */
    public enum WriteMethod {
      /**
       * Ingest write method. @see <a
       * href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.hl7V2Stores.messages/ingest></a>
       */
      INGEST,
      /**
       * Batch import write method. This is not yet supported by the HL7v2 API, but can be used to
       * improve throughput once available.
       */
      BATCH_IMPORT
    }

    /** The type Builder. */
    @AutoValue.Builder
    abstract static class Builder {

      /**
       * Sets HL7v2 store.
       *
       * @param hl7v2Store the HL7v2 store
       * @return the HL7v2 store
       */
      abstract Builder setHL7v2Store(String hl7v2Store);

      /**
       * Sets write method.
       *
       * @param writeMethod the write method
       * @return the write method
       */
      abstract Builder setWriteMethod(WriteMethod writeMethod);

      /**
       * Build write.
       *
       * @return the write
       */
      abstract Write build();
    }

    public static class Result implements POutput {
      private final Pipeline pipeline;
      private final PCollection<HealthcareIOError<HL7v2Message>> failedInsertsWithErr;

      /** Creates a {@link HL7v2IO.Write.Result} in the given {@link Pipeline}. */
      static Result in(
          Pipeline pipeline, PCollection<HealthcareIOError<HL7v2Message>> failedInserts) {
        return new Result(pipeline, failedInserts);
      }

      public PCollection<HealthcareIOError<HL7v2Message>> getFailedInsertsWithErr() {
        return this.failedInsertsWithErr;
      }

      @Override
      public Pipeline getPipeline() {
        return this.pipeline;
      }

      @Override
      public Map<TupleTag<?>, PValue> expand() {
        failedInsertsWithErr.setCoder(new HealthcareIOErrorCoder<>(new HL7v2MessageCoder()));
        return ImmutableMap.of(FAILED, failedInsertsWithErr);
      }

      @Override
      public void finishSpecifyingOutput(
          String transformName, PInput input, PTransform<?, ?> transform) {}

      private Result(
          Pipeline pipeline, PCollection<HealthcareIOError<HL7v2Message>> failedInsertsWithErr) {
        this.pipeline = pipeline;
        this.failedInsertsWithErr = failedInsertsWithErr;
      }
    }
  }

  /** The type Write hl 7 v 2. */
  static class WriteHL7v2 extends PTransform<PCollection<HL7v2Message>, Write.Result> {
    private final String hl7v2Store;
    private final Write.WriteMethod writeMethod;

    /**
     * Instantiates a new Write hl 7 v 2.
     *
     * @param hl7v2Store the hl 7 v 2 store
     * @param writeMethod the write method
     */
    WriteHL7v2(String hl7v2Store, Write.WriteMethod writeMethod) {
      this.hl7v2Store = hl7v2Store;
      this.writeMethod = writeMethod;
    }

    @Override
    public Write.Result expand(PCollection<HL7v2Message> input) {
      PCollection<HealthcareIOError<HL7v2Message>> failedInserts =
          input
              .apply(ParDo.of(new WriteHL7v2Fn(hl7v2Store, writeMethod)))
              .setCoder(new HealthcareIOErrorCoder<>(new HL7v2MessageCoder()));
      return Write.Result.in(input.getPipeline(), failedInserts);
    }

    /** The type Write hl 7 v 2 fn. */
    static class WriteHL7v2Fn extends DoFn<HL7v2Message, HealthcareIOError<HL7v2Message>> {
      // TODO when the healthcare API releases a bulk import method this should use that to improve
      // throughput.

      private Distribution messageIngestLatencyMs =
          Metrics.distribution(WriteHL7v2Fn.class, "message-ingest-latency-ms");
      private Counter failedMessageWrites =
          Metrics.counter(WriteHL7v2Fn.class, "failed-hl7v2-message-writes");
      private final String hl7v2Store;
      private final Counter successfulHL7v2MessageWrites =
          Metrics.counter(WriteHL7v2.class, "successful-hl7v2-message-writes");
      private final Write.WriteMethod writeMethod;

      private static final Logger LOG = LoggerFactory.getLogger(WriteHL7v2.WriteHL7v2Fn.class);
      private transient HealthcareApiClient client;

      /**
       * Instantiates a new Write HL7v2 fn.
       *
       * @param hl7v2Store the HL7v2 store
       * @param writeMethod the write method
       */
      WriteHL7v2Fn(String hl7v2Store, Write.WriteMethod writeMethod) {
        this.hl7v2Store = hl7v2Store;
        this.writeMethod = writeMethod;
      }

      /**
       * Init client.
       *
       * @throws IOException the io exception
       */
      @Setup
      public void initClient() throws IOException {
        this.client = new HttpHealthcareApiClient();
      }

      /**
       * Write messages.
       *
       * @param context the context
       */
      @ProcessElement
      public void writeMessages(ProcessContext context) {
        HL7v2Message msg = context.element();
        // all fields but data and labels should be null for ingest.
        Message model = new Message();
        model.setData(msg.getData());
        model.setLabels(msg.getLabels());
        switch (writeMethod) {
          case BATCH_IMPORT:
            // TODO once healthcare API exposes batch import API add that functionality here to
            // improve performance this should be the new default behavior/List.
            throw new UnsupportedOperationException("The Batch import API is not available yet");
          case INGEST:
          default:
            try {
              long requestTimestamp = Instant.now().getMillis();
              client.ingestHL7v2Message(hl7v2Store, model);
              messageIngestLatencyMs.update(Instant.now().getMillis() - requestTimestamp);
            } catch (Exception e) {
              failedMessageWrites.inc();
              LOG.warn(
                  String.format(
                      "Failed to ingest message Error: %s Stacktrace: %s",
                      e.getMessage(), Throwables.getStackTraceAsString(e)));
              HealthcareIOError<HL7v2Message> err = HealthcareIOError.of(msg, e);
              LOG.warn(String.format("%s %s", err.getErrorMessage(), err.getStackTrace()));
              context.output(err);
            }
        }
      }
    }
  }
}
