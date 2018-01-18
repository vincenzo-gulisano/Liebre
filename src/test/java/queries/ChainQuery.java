package queries;

import java.io.File;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import common.util.Util;
import dummy.DummyLatencyLogger;
import dummy.DummyMapFunction;
import dummy.DummySourceFunction;
import dummy.DummyTuple;
import dummy.RoundRobinTaskPool;
import operator.Operator;
import query.Query;
import query.QueryConfiguration;
import scheduling.Scheduler;
import scheduling.TaskPool;
import scheduling.impl.PriorityTaskPool;
import scheduling.impl.PriorityTaskPool2;
import scheduling.impl.ProbabilisticTaskPool;
import scheduling.impl.ThreadPoolScheduler;
import sink.Sink;
import source.Source;

//FIXME: Rename interval to quantum
public class ChainQuery {

	private static final double SELECTIVITY_MEAN = 0.9;
	private static final double SELECTIVITY_MAX_DIFF = 0.05;

	private static final int LOAD_MEAN = 100;
	private static final int LOAD_MAX_DIFF = 50;

	private static final String PROPERTY_FILENAME = "liebre.properties";
	private static final Random r = new Random(12);

	public static void main(String[] args) {

		if (args.length != 5) {
			throw new IllegalArgumentException(
					"Program requires 3 arguments: [output folder] [simulation duration (sec)] [load factor] [#chains] [#operators]");
		}
		// Parse Command Line Arguments
		final String statisticsFolder = args[0];
		final long queryDurationMillis = TimeUnit.SECONDS.toMillis(Long.parseLong(args[1]));
		final long loadFactor = Long.parseLong(args[2]);
		final int chainsNumber = Integer.parseInt(args[3]);
		final int operatorsNumber = Integer.parseInt(args[4]);

		// Configuration Init
		// TODO: Move all this inside the Query class
		Util.LOOPS_PER_MILLI = Math.round(Math.pow(10, loadFactor));
		final QueryConfiguration config = new QueryConfiguration(PROPERTY_FILENAME, SampleQuery.class);
		final TaskPool<Operator<?, ?>> pool;
		// Query creation
		Query q;
		if (config.isSchedulingEnabled()) { // If scheduling enabled, configure
			switch (config.getTaskPoolType()) {
			case 0:
				pool = new RoundRobinTaskPool();
				break;
			case 1:
				pool = new PriorityTaskPool(config.getPriorityMetric(), config.getHelperThreadsNumber(),
						config.getHelperThreadInterval());
				break;
			case 2:
				pool = new PriorityTaskPool2(config.getPriorityMetric(), config.getThreadsNumber());
				break;
			case 3:
				String priorityStatisticsFolder = config.isTaskPoolStatisticsEnabled() ? statisticsFolder : null;
				pool = new ProbabilisticTaskPool(config.getPriorityMetric(), config.getThreadsNumber(),
						config.getPriorityScalingFactor(), priorityStatisticsFolder);
				break;
			default:
				throw new IllegalArgumentException("Unknown TaskPool type!");
			}
			Scheduler scheduler = new ThreadPoolScheduler(config.getThreadsNumber(), config.getSchedulingInterval(),
					TimeUnit.MILLISECONDS, pool);
			q = new Query(scheduler);
		} else { // Otherwise, no scheduler
			q = new Query();
			pool = null;
		}

		q.activateStatistics(statisticsFolder);

		for (int i = 0; i < chainsNumber; i++) {
			addChain(i, operatorsNumber, q, statisticsFolder);
		}

		System.out.format("*** Starting large sample query with configuration: %s%n", config);
		System.out.format("*** Statistics folder: %s/%s%n", System.getProperty("user.dir"), statisticsFolder);
		System.out.format("*** Chains = %d%n", chainsNumber);
		System.out.format("*** Operators = %d%n", operatorsNumber);
		System.out.format("*** Duration: %s seconds%n", args[1]);
		System.out.format("*** Loops per millisec: %s%n", Util.LOOPS_PER_MILLI);

		// Start queries and let run for some time
		q.activate();
		Util.sleep(queryDurationMillis);
		q.deActivate();

	}

	private static double randomSelectivity() {
		double selectivity = SELECTIVITY_MEAN;
		double diff = r.nextDouble() % SELECTIVITY_MAX_DIFF;
		return r.nextBoolean() ? selectivity + diff : selectivity - diff;
	}

	private static long randomLoad() {
		long load = LOAD_MEAN;
		long diff = r.nextInt(LOAD_MAX_DIFF);
		return r.nextBoolean() ? load + diff : load - diff;
	}

	private static Operator<DummyTuple, DummyTuple> newMapOperator(int chainId, int opId, Query q) {
		String name = String.format("M%02d%02d", chainId, opId);
		Operator<DummyTuple, DummyTuple> map = q.addMapOperator(name,
				new DummyMapFunction(randomSelectivity(), randomLoad()));
		return map;
	}

	private static void addChain(int id, int nOperators, Query q, String statisticsFolder) {
		// Query Definition
		String sourceName = String.format("I%02d", id);
		String sinkName = String.format("S%02d", id);
		Source<DummyTuple> source = q.addBaseSource(sourceName, new DummySourceFunction(randomLoad()));
		Operator<DummyTuple, DummyTuple> firstMap = newMapOperator(id, 0, q);
		source.addOutput(firstMap);

		Operator<DummyTuple, DummyTuple> prev = firstMap;
		for (int i = 1; i < nOperators; i++) {
			Operator<DummyTuple, DummyTuple> m = newMapOperator(id, i, q);
			prev.addOutput(m);
			prev = m;
		}

		Sink<DummyTuple> sink = q.addBaseSink(sinkName,
				new DummyLatencyLogger(statisticsFolder + File.separator + "SINK_" + sinkName + ".latency.csv"));

		prev.addOutput(sink);
	}

}
