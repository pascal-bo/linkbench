/*
 * Copyright 2021-present MongoDB Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.LinkBench.stats;

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.text.DecimalFormat;

import com.facebook.LinkBench.LinkBenchOp;
import com.facebook.LinkBench.LinkStore;
import com.facebook.LinkBench.Logger;
import org.HdrHistogram.AtomicHistogram;

/**
 * Class used to track latency values using HDR Histogram.  This provides
 * a more accurate estimate of the latency percentiles at a cost of a higher
 * memory footprint than the default histogram type
 *
 * See http://hdrhistogram.org/ for more details
 */
public class HdrLatencyHistogram implements LatencyHistogram {

  private static final int percentiles[] = new int[] {25, 50, 75, 95, 99};
  private final AtomicHistogram histograms[];
  private final int maxThreads;

  public HdrLatencyHistogram(int maxThreads, int histogramAccuracy, int maxHistogramValue) {
    this.maxThreads = maxThreads;
    histograms = new AtomicHistogram[LinkStore.MAX_OPTYPES];
    for (LinkBenchOp op : LinkBenchOp.values()) {
      histograms[op.ordinal()] = new AtomicHistogram(maxHistogramValue, histogramAccuracy);
    }
  }

  /**
   * Get an estimate of the amount of memory that will be used by the histogram
   */
  public long memoryUsageEstimate() {
    long usageInBytes = 0;
    for(AtomicHistogram histogram : histograms)
    {
      usageInBytes += histogram.getEstimatedFootprintInBytes();
    }

    return usageInBytes;
  }

  /**
   * Track a latency value for a specific operation
   */
  @Override
  public void recordLatency(int threadid, LinkBenchOp type, long microtimetaken) {
    histograms[type.ordinal()].recordValue(microtimetaken);
  }

  /**
   * Print the current latency stats tracked to the logs
   */
  @Override
  public void displayLatencyStats() {
    Logger logger = Logger.getLogger();
    // print percentiles
    for (LinkBenchOp type: LinkBenchOp.values()) {

      AtomicHistogram histogram = histograms[type.ordinal()];
      if (histogram.getTotalCount() == 0) { // no samples of this type
        continue;
      }

      long sampleCounts = histogram.getTotalCount();
      String logString = type.displayName() + String.format(" count = %d ", sampleCounts);
      for(int percentile: percentiles) {
        logString += String.format(" p%d = %dus ", percentile, histogram.getValueAtPercentile(percentile));
      }

      double mean = histogram.getMean();
      long max = histogram.getMaxValue();
      logString += String.format(" max = %dus mean = %.3fus threads = %d", max, mean, maxThreads);
      logger.info(logString);
    }
  }

  /**
   * Print the current latency stats in csv format
   */
  @Override
  public void printCSVStats(PrintStream out, boolean header) {
    printCSVStats(out, header, LinkBenchOp.values());
  }

  @Override
  public void printHistogram(String outFileName, String phase, boolean header) {

    for (LinkBenchOp op: LinkBenchOp.values()) {
      AtomicHistogram histogram = histograms[op.ordinal()];
      try{
        String fullFileName = phase + "_" + outFileName + "_" + op.name() + ".txt";
        PrintStream out = new PrintStream(fullFileName);
        histogram.outputPercentileDistribution(out, 1.0);
      } catch (FileNotFoundException e){
        System.out.println("Could not open the file " + outFileName);
      }
    }

  }

  private void printCSVStats(PrintStream out, boolean header, LinkBenchOp... ops){

    // Write out the header
    if (header) {
      out.print("op,count");
      int last_percentile = 0;
      for (int percentile: percentiles) {
        out.print(String.format(",p%d (us)", percentile));
        out.print(String.format(",count from p%d to p%d", last_percentile, percentile));
        last_percentile = percentile;
      }

      out.print(",count from p99 to max-value");

      out.print(",max (us),mean (us),threads");
      out.println();
    }

    DecimalFormat df = new DecimalFormat("#.##");
    for (LinkBenchOp op: ops) {
      AtomicHistogram histogram = histograms[op.ordinal()];
      long samples = histogram.getTotalCount();
      if (samples == 0) {
        continue;
      }

//      Logger logger = Logger.getLogger();
//      logger.info("pascal start");
//      logger.info(String.valueOf(op));
//      for (HistogramIterationValue v : histogram.recordedValues()){
//        System.out.println("Percentile: " + v.getPercentile());
//        System.out.println(v);
//      }
//      logger.info(histogram.recordedValues().toString());
//      logger.info("pascal end");

      out.print(op.name());
      out.print(",");
      out.print(samples);
      double lastPercentileValue = 0;
      for (int percentile: percentiles) {
        double percentileValue = histogram.getValueAtPercentile(percentile);
        out.print(",");
        out.print(df.format(percentileValue));
        out.print(",");
        long lower = Long.parseLong(df.format(histogram.nextNonEquivalentValue(Long.parseLong(df.format(lastPercentileValue)))));
        long upper = Long.parseLong(df.format(percentileValue));

//        if (histogram.lowestEquivalentValue(lower) != lower) {
//          System.out.println("There is a mismatch at " + percentile + " at lower from " + op.name() + ": " + lower + " and " + histogram.lowestEquivalentValue(lower));
//        }
//
//        if (histogram.highestEquivalentValue(upper) != upper) {
//          System.out.println("There is a mismatch at " + percentile + " at upper from " + op.name() + ": " + upper + " and " + histogram.highestEquivalentValue(upper));
//        }

        out.print(df.format(histogram.getCountBetweenValues(lower, upper)));
        lastPercentileValue = percentileValue;
      }


      String max = df.format(histogram.getMaxValueAsDouble());
      long maxL = histogram.getMaxValue();
      String mean = df.format(histogram.getMean());

      out.print(",");
      long lower = Long.parseLong(df.format(histogram.nextNonEquivalentValue(Long.parseLong(df.format(lastPercentileValue)))));
      long upper = maxL;

//      System.out.println("lower:");
//      System.out.println(lower);
//      System.out.println("MAX:");
//      System.out.println(maxL);

//      if (histogram.lowestEquivalentValue(lower) != lower) {
//        System.out.println("There is a mismatch at max at lower from " + op.name() + ": " + lower + " and " + histogram.lowestEquivalentValue(lower));
//      }
//
//      if (histogram.highestEquivalentValue(upper) != upper) {
//        System.out.println("There is a mismatch at max at upper from " + op.name() + ": " + upper + " and " + histogram.highestEquivalentValue(upper));
//      }

      out.print(df.format(histogram.getCountBetweenValues(lower, upper)));

      out.print(",");
      out.print(max);
      out.print(",");
      out.print(mean);
      out.print(",");
      out.print(maxThreads);
      out.println();
    }
  }
}
