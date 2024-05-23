/*
 * Copyright 2012, Facebook, Inc.
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
package com.facebook.LinkBench;

import com.facebook.LinkBench.LinkBenchLoad.LoadChunk;
import com.facebook.LinkBench.LinkBenchLoad.LoadProgress;
import com.facebook.LinkBench.LinkBenchRequest.RequestProgress;
import com.facebook.LinkBench.stats.LatencyHistogram;
import com.facebook.LinkBench.stats.LatencyHistogramFactory;
import com.facebook.LinkBench.stats.SampledStats;
import com.facebook.LinkBench.util.ClassLoadUtil;
import org.apache.commons.cli.*;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
 LinkBenchDriver class.
 First loads data using multi-threaded LinkBenchLoad class.
 Then does read and write requests of various types (addlink, deletelink,
 updatelink, getlink, countlinks, getlinklist) using multi-threaded
 LinkBenchRequest class.
 Config options are taken from config file passed as argument.
 */

public class LinkBenchDriver {

  public static final int EXIT_BADARGS = 1;
  public static final int EXIT_BADCONFIG = 2;

  /* Command line arguments */
  private static String configFile = null;
  private static String workloadConfigFile = null;
  private static Properties cmdLineProps = null;
  private static String logFile = null;
  /** File for final statistics */
  private static PrintStream csvStatsFile = null;
  /** File for final histogram file */
  private static String histogramFileName = null;
  /** File for output of incremental csv data */
  private static PrintStream csvStreamFile = null;
  private static boolean doLoad = false;
  private static boolean doRequest = false;

  private Properties props;
  private int dbcount;

  private final Logger logger = Logger.getLogger();
  private final LatencyHistogramFactory latencyHistogramFactory = new LatencyHistogramFactory(logger);

  LinkBenchDriver(String configfile, Properties
                  overrideProps, String logFile)
    throws java.io.FileNotFoundException, IOException, LinkBenchConfigError {
    // which link store to use
    props = new Properties();
    props.load(new FileInputStream(configfile));
    for (String key: overrideProps.stringPropertyNames()) {
      props.setProperty(key, overrideProps.getProperty(key));
    }

    loadWorkloadProps();

    ConfigUtil.setupLogging(props, logFile);
    dbcount = ConfigUtil.getInt(props, Config.DBCOUNT, 1);

    logger.info("Config file: " + configfile);
    logger.info("Workload config file: " + workloadConfigFile);
  }

  /**
   * Load properties from auxilliary workload properties file if provided.
   * Properties from workload properties file do not override existing
   * properties
   * @throws IOException
   * @throws FileNotFoundException
   */
  private void loadWorkloadProps() throws IOException, FileNotFoundException {
    if (props.containsKey(Config.WORKLOAD_CONFIG_FILE)) {
      workloadConfigFile = props.getProperty(Config.WORKLOAD_CONFIG_FILE);
      if (!new File(workloadConfigFile).isAbsolute()) {
        String linkBenchHome = ConfigUtil.findLinkBenchHome();
        if (linkBenchHome == null) {
          throw new RuntimeException("Data file config property "
              + Config.WORKLOAD_CONFIG_FILE
              + " was specified using a relative path, but linkbench home"
              + " directory was not specified through environment var "
              + ConfigUtil.linkbenchHomeEnvVar);
        } else {
          workloadConfigFile = linkBenchHome + File.separator + workloadConfigFile;
        }
      }
      Properties workloadProps = new Properties();
      workloadProps.load(new FileInputStream(workloadConfigFile));
      // Add workload properties, but allow other values to override
      for (String key: workloadProps.stringPropertyNames()) {
        if (props.getProperty(key) == null) {
          props.setProperty(key, workloadProps.getProperty(key));
        }
      }
    }
  }

  private static class Stores {
    final LinkStore linkStore;
    final NodeStore nodeStore;
    public Stores(LinkStore linkStore, NodeStore nodeStore) {
      super();
      this.linkStore = linkStore;
      this.nodeStore = nodeStore;
    }
  }

  // generate instances of LinkStore and NodeStore
  private Stores initStores()
    throws Exception {
    LinkStore linkStore = createLinkStore();
    NodeStore nodeStore = createNodeStore(linkStore);

    return new Stores(linkStore, nodeStore);
  }

  private LinkStore createLinkStore() throws Exception, IOException {
    // The property "linkstore" defines the class name that will be used to
    // store data in a database. The folowing class names are pre-packaged
    // for easy access:
    //   LinkStoreMysql :  run benchmark on  mySQL
    //   LinkStoreHBaseGeneralAtomicityTesting : atomicity testing on HBase.

    String linkStoreClassName = ConfigUtil.getPropertyRequired(props,
                                            Config.LINKSTORE_CLASS);

    logger.debug("Using LinkStore implementation: " + linkStoreClassName);

    LinkStore linkStore;
    try {
      linkStore = ClassLoadUtil.newInstance(linkStoreClassName,
                                            LinkStore.class);
    } catch (ClassNotFoundException nfe) {
      throw new IOException("Cound not find class for " + linkStoreClassName);
    }

    return linkStore;
  }

  /**
   * @param linkStore a LinkStore instance to be reused if it turns out
   * that linkStore and nodeStore classes are same
   * @return
   * @throws Exception
   * @throws IOException
   */
  private NodeStore createNodeStore(LinkStore linkStore) throws Exception,
      IOException {
    String nodeStoreClassName = props.getProperty(Config.NODESTORE_CLASS);
    if (nodeStoreClassName == null) {
      logger.debug("No NodeStore implementation provided");
    } else {
      logger.debug("Using NodeStore implementation: " + nodeStoreClassName);
    }

    if (linkStore != null && linkStore.getClass().getName().equals(
                                                nodeStoreClassName)) {
      // Same class, reuse object
      if (!NodeStore.class.isAssignableFrom(linkStore.getClass())) {
        throw new Exception("Specified NodeStore class " + nodeStoreClassName
                          + " is not a subclass of NodeStore");
      }
      return (NodeStore)linkStore;
    } else {
      NodeStore nodeStore;
      try {
        nodeStore = ClassLoadUtil.newInstance(nodeStoreClassName,
                                                            NodeStore.class);
        return nodeStore;
      } catch (java.lang.ClassNotFoundException nfe) {
        throw new IOException("Cound not find class for " + nodeStoreClassName);
      }
    }
  }

  void load() throws IOException, InterruptedException, Throwable {

    if (!doLoad) {
      logger.info("Skipping load data per the cmdline arg");
      return;
    }
    // load data
    int nLinkLoaders = ConfigUtil.getInt(props, Config.NUM_LOADERS);


    boolean bulkLoad = true;
    ConcurrentLinkedQueue<LinkedBlockingQueue<LoadChunk>> chunk_q_list = new ConcurrentLinkedQueue<>();

    // max id1 to generate
    long maxid1 = ConfigUtil.getLong(props, Config.MAX_ID);
    // id1 at which to start
    long startid1 = ConfigUtil.getLong(props, Config.MIN_ID);

    boolean neverChange = false;
    if (props.containsKey(Config.NEVER_CHANGE))
      neverChange = ConfigUtil.getBool(props, Config.NEVER_CHANGE);

    // Create loaders
    logger.info("Starting loaders " + nLinkLoaders);
    logger.debug("Bulk Load setting: " + bulkLoad);

    Random masterRandom = createMasterRNG(props, Config.LOAD_RANDOM_SEED);


    boolean genNodes = ConfigUtil.getBool(props, Config.GENERATE_NODES);
    int nTotalLoaders = genNodes ? nLinkLoaders + 1 : nLinkLoaders;

    boolean genNodesFirst = ConfigUtil.getBool(props, Config.GENERATE_NODES_FIRST, false);

    LatencyHistogram latencyStats = latencyHistogramFactory.create(nTotalLoaders, props);
    List<Runnable> linkLoaders = new ArrayList<Runnable>(nTotalLoaders);

    LoadProgress loadTracker = LoadProgress.create(logger, props);
    for (int i = 0; i < nLinkLoaders; i++) {
      LinkStore linkStore = createLinkStore();

      bulkLoad = bulkLoad && linkStore.bulkLoadBatchSize() > 0;
      LinkBenchLoad l = new LinkBenchLoad(linkStore, props, latencyStats,
              csvStreamFile, i, maxid1 == startid1 + 1, chunk_q_list, loadTracker);
      linkLoaders.add(l);
    }

    List<NodeLoader> nodeLoaders = new ArrayList<>();
    if (genNodes) {
      logger.info("Will generate graph nodes during loading");
      int loaderId = nTotalLoaders - 1;
      NodeStore nodeStore = createNodeStore(null);
      Random rng = new Random(masterRandom.nextLong());
      nodeLoaders.add(new NodeLoader(props, logger, nodeStore, rng,
          latencyStats, csvStreamFile, loaderId));
    }
    enqueueLoadWork(chunk_q_list, startid1, maxid1, nLinkLoaders,
                    new Random(masterRandom.nextLong()));

    long loadTime = 0;
    long expectedNodes = maxid1 - startid1;
    long actualLinks = 0;
    long actualCounts = 0;
    long actualNodes = 0;

    // run loaders
    loadTracker.startTimer();
    if (genNodesFirst) {
      loadTime = concurrentExec(nodeLoaders);
      for (final NodeLoader nl : nodeLoaders) {
          actualNodes += nl.getNodesLoaded();
      }

      loadTime += concurrentExec(linkLoaders);
      for (final Runnable ll : linkLoaders) {
          actualLinks += ((LinkBenchLoad) ll).getLinksLoaded();
          actualCounts += ((LinkBenchLoad) ll).getCountsLoaded();
      }
    } else {
      List<Runnable> loaders = Stream.of(linkLoaders, nodeLoaders)
              .flatMap(Collection::stream)
              .collect(Collectors.toList());
      loadTime = concurrentExec(loaders);

      for (final Runnable l : loaders) {
        if (l instanceof LinkBenchLoad) {
          actualLinks += ((LinkBenchLoad) l).getLinksLoaded();
          actualCounts += ((LinkBenchLoad) l).getCountsLoaded();
        } else {
          assert (l instanceof NodeLoader);
          actualNodes += ((NodeLoader) l).getNodesLoaded();
        }
      }
    }

    GraphStore.printMetrics(logger);
    latencyStats.displayLatencyStats();

    if (csvStatsFile != null) {
      latencyStats.printCSVStats(csvStatsFile, true);
      latencyStats.printHistogram(histogramFileName, "load", true);
    }

    double loadTime_s = (loadTime/1000.0);
    if (neverChange) {
      logger.info(String.format("LOAD PHASE COMPLETED. " +
          " Loaded %d nodes (Expected %d)." +
          " Loaded %d links (%.2f links per node). " +
          " Took %.1f seconds.  Links/second = %d",
          actualNodes, expectedNodes, actualLinks,
          actualLinks / (double) actualNodes, loadTime_s,
          (long) Math.round(actualLinks / loadTime_s)));
    } else {
      logger.info(String.format("LOAD PHASE COMPLETED." +
          " Loaded %d nodes (Expected %d)," +
          " %d links (%.2f /node)," +
          " %d counts (%.2f /node)." +
          " %.1f seconds, %d Links/s, %d (link+count)/s, %d (link+count+node)/s",
          actualNodes, expectedNodes,
          actualLinks, actualLinks / (double) actualNodes,
          actualCounts, actualCounts / (double) actualNodes,
          loadTime_s,
          (long) Math.round(actualLinks / loadTime_s),
          (long) Math.round((actualLinks+actualCounts) / loadTime_s),
          (long) Math.round((actualLinks+actualCounts+actualNodes) / loadTime_s)));
    }
  }

  /**
   * Create a new random number generated, optionally seeded to a known
   * value from the config file.  If seed value not provided, a seed
   * is chosen.  In either case the seed is logged for later reproducibility.
   * @param props
   * @param configKey config key for the seed value
   * @return
   */
  private Random createMasterRNG(Properties props, String configKey) {
    long seed;
    if (props.containsKey(configKey)) {
      seed = ConfigUtil.getLong(props, configKey);
      logger.info("Using configured random seed " + configKey + "=" + seed);
    } else {
      seed = System.nanoTime() ^ (long)configKey.hashCode();
      logger.info("Using random seed " + seed + " since " + configKey
          + " not specified");
    }

    SecureRandom masterRandom;
    try {
      masterRandom = SecureRandom.getInstance("SHA1PRNG");
    } catch (NoSuchAlgorithmException e) {
      logger.warn("SHA1PRNG not available, defaulting to default SecureRandom" +
          " implementation");
      masterRandom = new SecureRandom();
    }
    masterRandom.setSeed(ByteBuffer.allocate(8).putLong(seed).array());

    // Can be used to check that rng is behaving as expected
    logger.debug("First number generated by master " + configKey +
                 ": " + masterRandom.nextLong());
    return masterRandom;
  }

  private void enqueueLoadWork(ConcurrentLinkedQueue<LinkedBlockingQueue<LoadChunk>> chunk_q_list,
                               long startid1, long maxid1, int nloaders, Random rng) {
    // Enqueue work chunks.  Do it in reverse order as a heuristic to improve
    // load balancing, since queue is FIFO and later chunks tend to be larger

    int chunkSize = ConfigUtil.getInt(props, Config.LOADER_CHUNK_SIZE, 2048);
    for (int count = 0; count < dbcount; ++count) {
      LinkedBlockingQueue<LoadChunk> chunk_q = new LinkedBlockingQueue<>();
      long chunk_num = 0;
      ArrayList<LoadChunk> stack = new ArrayList<LoadChunk>();
      for (long id1 = startid1; id1 < maxid1; id1 += chunkSize) {
        stack.add(new LoadChunk(chunk_num, id1,
                Math.min(id1 + chunkSize, maxid1), rng));
        chunk_num++;
      }

      for (int i = stack.size() - 1; i >= 0; i--) {
        chunk_q.add(stack.get(i));
      }

      for (int i = 0; i < nloaders; i++) {
        // Add a shutdown signal for each loader
        chunk_q.add(LoadChunk.SHUTDOWN);
      }
      chunk_q_list.add(chunk_q);
    }
  }

  void sendrequests() throws IOException, InterruptedException, Throwable {

    if (!doRequest) {
      logger.info("Skipping request phase per the cmdline arg");
      return;
    }

    // config info for requests
    int nrequesters = ConfigUtil.getInt(props, Config.NUM_REQUESTERS);
    if (nrequesters == 0) {
      logger.info("NO REQUEST PHASE CONFIGURED. ");
      return;
    }
    LatencyHistogram latencyStats = latencyHistogramFactory.create(nrequesters, props);
    List<LinkBenchRequest> requesters = new LinkedList<LinkBenchRequest>();

    RequestProgress progress = LinkBenchRequest.createProgress(logger, props);

    Random masterRandom = createMasterRNG(props, Config.REQUEST_RANDOM_SEED);

    // create requesters
    for (int i = 0; i < nrequesters; i++) {
      Stores stores = initStores();
      LinkBenchRequest l = new LinkBenchRequest(stores.linkStore,
              stores.nodeStore, props, latencyStats, csvStreamFile,
              progress, new Random(masterRandom.nextLong()), i, nrequesters);
      requesters.add(l);
    }
    progress.startTimer();
    // run requesters
    concurrentExec(requesters);
    long finishTime = System.currentTimeMillis();
    // Calculate duration accounting for warmup time
    long benchmarkTime = finishTime - progress.getBenchmarkStartTime();

    long requestsdone = 0;
    int abortedRequesters = 0;
    // wait for requesters
    for (LinkBenchRequest requester: requesters) {
      requestsdone += requester.getRequestsDone();
      if (requester.didAbort()) {
        abortedRequesters++;
      }
    }

    GraphStore.printMetrics(logger);
    latencyStats.displayLatencyStats();

    if (csvStatsFile != null) {
      latencyStats.printCSVStats(csvStatsFile, true);
      latencyStats.printHistogram(histogramFileName, "request", true);
    }

    logger.info("REQUEST PHASE COMPLETED. " + requestsdone +
                 " requests done in " + (benchmarkTime/1000) + " seconds." +
                 " Requests/second = " + (1000*requestsdone)/benchmarkTime);
    if (abortedRequesters > 0) {
      logger.error(String.format("Benchmark did not complete cleanly: %d/%d " +
          "request threads aborted.  See error log entries for details.",
          abortedRequesters, nrequesters));
    }
  }

  /**
   * Start all runnables at the same time. Then block till all
   * tasks are completed. Returns the elapsed time (in millisec)
   * since the start of the first task to the completion of all tasks.
   */
  static long concurrentExec(final List<? extends Runnable> tasks)
      throws Throwable {
    final CountDownLatch startSignal = new CountDownLatch(tasks.size());
    final CountDownLatch doneSignal = new CountDownLatch(tasks.size());
    final AtomicLong startTime = new AtomicLong(0);
    for (final Runnable task : tasks) {
      new Thread(new Runnable() {
        @Override
        public void run() {
          /*
           * Run a task.  If an uncaught exception occurs, bail
           * out of the benchmark immediately, since any results
           * of the benchmark will no longer be valid anyway
           */
          try {
            startSignal.countDown();
            startSignal.await();
            long now = System.currentTimeMillis();
            startTime.compareAndSet(0, now);
            task.run();
          } catch (Throwable e) {
            Logger threadLog = Logger.getLogger();
            threadLog.error("Unrecoverable exception in worker thread:" + e);
            Runtime.getRuntime().halt(1);
          }
          doneSignal.countDown();
        }
      }).start();
    }
    doneSignal.await(); // wait for all threads to finish
    long endTime = System.currentTimeMillis();
    return endTime - startTime.get();
  }

  void drive() throws IOException, InterruptedException, Throwable {
    load();
    sendrequests();
  }

  public static void main(String[] args)
    throws IOException, InterruptedException, Throwable {
    processArgs(args);
    LinkBenchDriver d = new LinkBenchDriver(configFile,
                                cmdLineProps, logFile);
    try {
      d.drive();
    } catch (LinkBenchConfigError e) {
      System.err.println("Configuration error: " + e.toString());
      System.exit(EXIT_BADCONFIG);
    }
  }

  private static void printUsage(Options options) {
    //PrintWriter writer = new PrintWriter(System.err);
    HelpFormatter fmt = new HelpFormatter();
    fmt.printHelp("linkbench", options, true);
  }

  private static Options initializeOptions() {
    Options options = new Options();
    Option config = new Option("c", true, "Linkbench config file");
    config.setArgName("file");
    options.addOption(config);

    Option log = new Option("L", true, "Log to this file");
    log.setArgName("file");
    options.addOption(log);

    Option csvStats = new Option("csvstats", "csvstats", true,
                                 "CSV stats output");
    csvStats.setArgName("file");
    options.addOption(csvStats);

    Option csvStream = new Option("csvstream", "csvstream", true,
        "CSV streaming stats output");
    csvStream.setArgName("file");
    options.addOption(csvStream);

    Option histogramFile = new Option("histogram", "histogram", true,
            "Histogram output");
    histogramFile.setArgName("file");
    options.addOption(histogramFile);

    options.addOption("l", false,
               "Execute loading stage of benchmark");
    options.addOption("r", false,
               "Execute request stage of benchmark");

    // Java-style properties to override config file
    // -Dkey=value
    Option property = new Option("D", "Override a config setting");
    property.setArgs(2);
    property.setArgName("property=value");
    property.setValueSeparator('=');
    options.addOption(property);

    return options;
  }

  /**
   * Process command line arguments and set static variables
   * exits program if invalid arguments provided
   * @param args
   * @throws ParseException
   */
  private static void processArgs(String[] args)
              throws ParseException {
    Options options = initializeOptions();

    CommandLine cmd = null;
    try {
      CommandLineParser parser = new GnuParser();
      cmd = parser.parse( options, args);
    } catch (ParseException ex) {
      // Use Apache CLI-provided messages
      System.err.println(ex.getMessage());
      printUsage(options);
      System.exit(EXIT_BADARGS);
    }

    /*
     * Apache CLI validates arguments, so can now assume
     * all required options are present, etc
     */
    if (cmd.getArgs().length > 0) {
      System.err.print("Invalid trailing arguments:");
      for (String arg: cmd.getArgs()) {
        System.err.print(' ');
        System.err.print(arg);
      }
      System.err.println();
      printUsage(options);
      System.exit(EXIT_BADARGS);
    }

    // Set static option variables
    doLoad = cmd.hasOption('l');
    doRequest = cmd.hasOption('r');

    logFile = cmd.getOptionValue('L'); // May be null

    configFile = cmd.getOptionValue('c');
    if (configFile == null) {
      // Try to find in usual location
      String linkBenchHome = ConfigUtil.findLinkBenchHome();
      if (linkBenchHome != null) {
        configFile = linkBenchHome + File.separator +
              "config" + File.separator + "LinkConfigMysql.properties";
      } else {
        System.err.println("Config file not specified through command "
            + "line argument and " + ConfigUtil.linkbenchHomeEnvVar
            + " environment variable not set to valid directory");
        printUsage(options);
        System.exit(EXIT_BADARGS);
      }
    }

    String csvStatsFileName = cmd.getOptionValue("csvstats"); // May be null
    if (csvStatsFileName != null) {
      try {
        csvStatsFile = new PrintStream(new FileOutputStream(csvStatsFileName));
      } catch (FileNotFoundException e) {
        System.err.println("Could not open file " + csvStatsFileName +
                           " for writing");
        printUsage(options);
        System.exit(EXIT_BADARGS);
      }
    }

    String histogramFileName = cmd.getOptionValue("histogram"); // May be null
    if (histogramFileName != null) {
        LinkBenchDriver.histogramFileName = histogramFileName;
    }

    String csvStreamFileName = cmd.getOptionValue("csvstream"); // May be null
    if (csvStreamFileName != null) {
      try {
        csvStreamFile = new PrintStream(
                        new FileOutputStream(csvStreamFileName));
        // File is written to by multiple threads, first write header
        SampledStats.writeCSVHeader(csvStreamFile);
      } catch (FileNotFoundException e) {
        System.err.println("Could not open file " + csvStreamFileName +
                           " for writing");
        printUsage(options);
        System.exit(EXIT_BADARGS);
      }
    }

    cmdLineProps = cmd.getOptionProperties("D");

    if (!(doLoad || doRequest)) {
      System.err.println("Did not select benchmark mode");
      printUsage(options);
      System.exit(EXIT_BADARGS);
    }
  }
}


