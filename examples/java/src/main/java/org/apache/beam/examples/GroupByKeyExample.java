package org.apache.beam.examples;

import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Random;

public class GroupByKeyExample {
  private static final Logger LOG = LoggerFactory.getLogger(GroupByKeyExample.class);


  public static interface Options extends PipelineOptions {
    @Description("The type of test case scenario")
    @Validation.Required
    String getTestCaseScenario();

    void setTestCaseScenario(String value);

    @Description("Size of no of values per key.")
    int getNoOfValuesPerKey();

    void setNoOfValuesPerKey(int value);

    @Description("Size of each value.")
    int getValueSize();

    void setValueSize(int value);

    @Description("No of keys.")
    int getNoOfKeys();

    void setNoOfKeys(int value);
  }

  private static int retrieveNumberFromKey(String key) {
    List<String> parts = Splitter.on('-').splitToList(key);
    return Integer.parseInt(parts.get(1));
  }

  public static class GenerateDataFn extends DoFn<String, KV<String, String>> {
    int totalValues = 500000;
    int stringLength = 2000000;

    GenerateDataFn(int totalValues, int stringLength) {
      this.totalValues = totalValues;
      this.stringLength = stringLength;
      LOG.error("Total No of Values: " + this.totalValues);
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws InterruptedException {
      String candidateChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
      int count = 0;
      for (int i = 0; i < totalValues; i++) {
        count++;
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < stringLength; j++) {
          sb.append(candidateChars.charAt(random.nextInt(candidateChars.length())));
        }
        sb.append(c.element());
        String str = sb.toString();
        KV<String, String> kv = KV.of(c.element(), str);
        c.output(kv);
      }
      LOG.error("The loop ran for: " + count + " no. of times.");
    }
  }

  public static class GenerateMixFn extends DoFn<String, KV<String, String>> {
    int totalValues = 10;
    int nonLargeIterableKeyStringLength = 200000;
    int largeIterableKeyStringLength = 2000000;

    GenerateMixFn(int totalValues) {
      this.totalValues = totalValues;
    }

    @ProcessElement
    public void processElement(ProcessContext c) throws InterruptedException {
      int stringLength = this.largeIterableKeyStringLength;
      String candidateChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
      if (retrieveNumberFromKey(c.element()) % 2 == 0) {
        stringLength = this.nonLargeIterableKeyStringLength;
      }
      for (int i = 0; i < this.totalValues; i++) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < stringLength; j++) {
          sb.append(candidateChars.charAt(random.nextInt(candidateChars.length())));
        }
        String str = sb.toString();
        KV<String, String> kv = KV.of(c.element(), str);
        c.output(kv);
      }
    }
  }

  public static class GenerateMixFnForSplit extends DoFn<String, KV<String, String>> {

    @ProcessElement
    public void processElement(ProcessContext c) throws InterruptedException {
      int stringLength = 4000000;
      int totalValues = 1;
      String candidateChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
      if (retrieveNumberFromKey(c.element()) >= 9) {
        totalValues = 101;
      }
      for (int i = 0; i < totalValues; i++) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int j = 0; j < stringLength; j++) {
          sb.append(candidateChars.charAt(random.nextInt(candidateChars.length())));
        }
        String str = sb.toString();
        KV<String, String> kv = KV.of(c.element(), str);
        c.output(kv);
      }
    }
  }

  public static class ShuffleSplitFn extends DoFn<KV<String, Iterable<String>>, KV<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext c) {
      Iterable<String> values = c.element().getValue();
      if (retrieveNumberFromKey(c.element().getKey()) < 9) {
        for (String value : values) {
          KV<String, String> kv = KV.of(c.element().getKey(), value);
          c.output(kv);
        }
      } else {
        int count = 1;
        for (String value : values) {
          KV<String, String> kv = KV.of(c.element().getKey(), value);
          if (count % 5 == 0) {
            try {
              // LOG.error("Hitting 50s timeout");
              Thread.sleep(10000);
            } catch (InterruptedException ie) {
              LOG.error("In catch.");
              continue;
            }
          }
          c.output(kv);
          count++;
        }
      }

    }
  }

  public static class ShuffleFn extends DoFn<KV<String, Iterable<String>>, KV<String, String>> {

    private Counter throttlingMsecs =
        Metrics.counter("org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl$StorageClientImpl", "throttling-msecs");

    @ProcessElement
    public void processElement(ProcessContext c) {
      Iterable<String> values = c.element().getValue();
      for (int i = 0; i < 1; i++) {
        for (String value : values) {
          KV<String, String> kv = KV.of(c.element().getKey(), value);
          c.output(kv);
        }
        LOG.error("Iteration complete for " + c.element().getKey());
      }
      // inject throttling time
      throttlingMsecs.inc(1);

    }
  }

  public static class HighLatencyBwStateRequestsFn
      extends DoFn<KV<String, Iterable<String>>, KV<String, String>> {

    // private Counter throttlingMsecs =
    //     Metrics.counter("org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl$StorageClientImpl", "throttling-msecs");
    @ProcessElement
    public void processElement(ProcessContext c) {
      Iterable<String> values = c.element().getValue();
      if (retrieveNumberFromKey(c.element().getKey()) % 2 == 0) {
        int count = 0;
        for (String value : values) {
          if (count == 2 || count == 4) {
            try {
              // LOG.error("Hitting 50s timeout");
              Thread.sleep(80000);
            } catch (InterruptedException ie) {
              LOG.error("In catch.");
              continue;
            }
          }
          // LOG.error("Count in even key:" + count);
          KV<String, String> kv = KV.of(c.element().getKey(), value);
          c.output(kv);
          count++;
        }
      } else {
        // int count_odd = 0;
        for (String value : values) {
          // LOG.error("Count in even key:" + count_odd);
          KV<String, String> kv = KV.of(c.element().getKey(), value);
          c.output(kv);
          //count_odd++;
        }
      }

    }
  }

  public static class MixLatencyNoIterationRequest
      extends DoFn<KV<String, Iterable<String>>, KV<String, String>> {

    // private Counter throttlingMsecs =
    //     Metrics.counter("org.apache.beam.sdk.io.gcp.bigquery.BigQueryServicesImpl$StorageClientImpl", "throttling-msecs");
    @ProcessElement
    public void processElement(ProcessContext c) {
      Iterable<String> values = c.element().getValue();
      int key = retrieveNumberFromKey(c.element().getKey());
      if (key == 0 || key == 2 || key == 6 || key == 8) {
        int count = 0;
        for (String value : values) {
          if (count == 2 || count == 4 || count == 6 || count == 8) {
            try {
              // LOG.error("Hitting 50s timeout");
              Thread.sleep(80000);
            } catch (InterruptedException ie) {
              LOG.error("In catch.");
              continue;
            }
          }
          // LOG.error("Count in even key:" + count);
          KV<String, String> kv = KV.of(c.element().getKey(), value);
          c.output(kv);
          count++;
        }
      } else if (key == 1 || key == 3 || key == 5) {
        for (String value : values) {
          KV<String, String> kv = KV.of(c.element().getKey(), value);
          c.output(kv);
        }
      }

    }
  }

  public static void main(String[] args) {
    Options options =
        (Options) PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    Pipeline p = Pipeline.create(options);
    ArrayList<String> listOfKeys = new ArrayList<String>();
    LOG.error("before the key loop: " + options.getNoOfKeys());
    for (int i = 0; i < options.getNoOfKeys(); i++) {
      LOG.error("We are at Key: " + i);
      listOfKeys.add("Key-" + String.valueOf(i));
    }
    PCollection<KV<String, String>> keyValues;
    if (options.getTestCaseScenario().contains("generate_mix_normal_shuffle")) {
      keyValues =
          p.apply(Create.of(listOfKeys))
              .apply(ParDo.of(new GenerateMixFn(options.getNoOfValuesPerKey())));
    } else if (options.getTestCaseScenario().contains("split_case")) {
      keyValues = p.apply(Create.of(listOfKeys)).apply(ParDo.of(new GenerateMixFnForSplit()));
    }
    else {
      keyValues =
          p.apply(Create.of(listOfKeys))
              .apply(
                  ParDo.of(new GenerateDataFn(options.getNoOfValuesPerKey(), options.getValueSize())));
    }

    PCollection<KV<String, Iterable<String>>> groupedValues = keyValues.apply(GroupByKey.create());
    if (options.getTestCaseScenario().contains("normal_shuffle")) {
      groupedValues.apply(ParDo.of(new ShuffleFn()));
    }
    if (options.getTestCaseScenario().contains("chained_shuffle")) {
      groupedValues
          .apply(ParDo.of(new ShuffleFn()))
          .apply(GroupByKey.create())
          .apply(ParDo.of(new ShuffleFn()));
    }
    if (options.getTestCaseScenario().contains("sdk_no_use_elements")) {
      groupedValues.apply(
          ParDo.of(
              new DoFn<KV<String, Iterable<String>>, Long>() {
                @ProcessElement
                public void processElement(ProcessContext c) {}
              }));
    }
    if (options.getTestCaseScenario().contains("high_latency_bw_state_requests")) {
      groupedValues.apply(ParDo.of(new HighLatencyBwStateRequestsFn()));
    }
    if (options.getTestCaseScenario().contains("split_case")) {
      groupedValues.apply(ParDo.of(new ShuffleSplitFn()));
    }
    if (options.getTestCaseScenario().contains("mix_no_iteration")) {
      groupedValues.apply(ParDo.of(new MixLatencyNoIterationRequest()));
    }
    p.run();
  }
}

