package queries;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import common.util.Util;
import dummy.DummyLatencyLogger;
import dummy.DummyMapFunction;
import dummy.DummyRouterFunction;
import dummy.DummySourceFunction;
import dummy.DummyTuple;
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
public class LargeQuery {
	private static class ProcessingRate {
		// Query 1
		static final long I1 = 150;
		static final long MAP = 100;
		static final long ROUTER = 80;

		static final long MAP_FINAL_SLOW = 100;
		static final long MAP_FINAL = 20;

		private ProcessingRate() {
		}
	}

	private static class Selectivity {
		// Query 1
		static final double MAP = 1.0;
		static final double ROUTER = 1.0;
		static final double MAP_FINAL = 1.0;

		private Selectivity() {
		}
	}

	private static final String PROPERTY_FILENAME = "liebre.properties";

	public static void main(String[] args) {

		if (args.length != 4) {
			throw new IllegalArgumentException(
					"Program requires 4 arguments: [output folder] [simulation duration (sec)] [#sinks] [#slow_sinks]");
		}
		// Parse Command Line Arguments
		final String statisticsFolder = args[0];
		final long queryDurationMillis = TimeUnit.SECONDS.toMillis(Long.parseLong(args[1]));
		final int sinksNumber = Integer.parseInt(args[2]);
		final int slowSinksNumber = Integer.parseInt(args[3]);

		// Configuration Init
		// TODO: Move all this inside the Query class
		final QueryConfiguration config = new QueryConfiguration(PROPERTY_FILENAME, SampleQuery.class);
		final TaskPool<Operator<?, ?>> pool;
		// Query creation
		Query q;
		if (config.isSchedulingEnabled()) { // If scheduling enabled, configure
			switch (config.getTaskPoolType()) {
			case 1:
				pool = new PriorityTaskPool(config.getPriorityMetric(), config.getHelperThreadsNumber(),
						config.getHelperThreadInterval());
				break;
			case 2:
				pool = new PriorityTaskPool2(config.getPriorityMetric());
				break;
			case 3:
				pool = new ProbabilisticTaskPool(config.getPriorityMetric(), config.getThreadsNumber(),
						config.getPriorityScalingFactor());
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
		Source<DummyTuple> source = q.addBaseSource("I0", new DummySourceFunction(ProcessingRate.I1));
		// Create and connect operators
		Operator<DummyTuple, DummyTuple> map = q.addMapOperator("MAP",
				new DummyMapFunction(Selectivity.MAP, ProcessingRate.MAP));

		source.addOutput(map);

		List<Operator<DummyTuple, DummyTuple>> finalOperators = new ArrayList<>();
		List<String> chosenIds = new ArrayList<String>();

		for (int i = 0; i < sinksNumber; i++) {
			long pR = i < slowSinksNumber ? ProcessingRate.MAP_FINAL_SLOW : ProcessingRate.MAP_FINAL;
			Operator<DummyTuple, DummyTuple> temp = q.addMapOperator("FINAL" + i,
					new DummyMapFunction(Selectivity.MAP_FINAL, pR));
			finalOperators.add(temp);
			chosenIds.add(temp.getId());
		}

		Operator<DummyTuple, DummyTuple> router = q.addRouterOperator("ROUTER",
				new DummyRouterFunction(Selectivity.ROUTER, ProcessingRate.ROUTER, chosenIds));

		map.addOutput(router);

		for (Operator<DummyTuple, DummyTuple> op : finalOperators) {
			router.addOutput(op);
			String sinkId = "O" + op.getId();
			Sink<DummyTuple> sink = q.addBaseSink(sinkId,
					new DummyLatencyLogger(statisticsFolder + File.separator + sinkId + ".latency.csv"));
			op.addOutput(sink);
		}

		System.out.format("*** Starting large sample query with configuration: %s%n", config);
		System.out.format("*** Statistics folder: %s/%s%n", System.getProperty("user.dir"), statisticsFolder);
		System.out.format("*** Sinks = %d%n", sinksNumber);
		System.out.format("*** Slow sinks = %d%n", slowSinksNumber);
		System.out.format("*** Duration: %s seconds%n", args[1]);

		ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
		assert threadMXBean.isThreadCpuTimeSupported();
		assert threadMXBean.isCurrentThreadCpuTimeSupported();

		threadMXBean.setThreadContentionMonitoringEnabled(true);
		threadMXBean.setThreadCpuTimeEnabled(true);
		assert threadMXBean.isThreadCpuTimeEnabled();

		// Start queries and let run for some time
		q.activate();

		Util.sleep(queryDurationMillis);

		ThreadInfo[] threadInfo = threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds());
		for (ThreadInfo threadInfo2 : threadInfo) {
			long blockedTime = threadInfo2.getBlockedTime();
			long waitedTime = threadInfo2.getWaitedTime();
			long cpuTime = TimeUnit.NANOSECONDS.toMillis(threadMXBean.getThreadCpuTime(threadInfo2.getThreadId()));
			long userTime = TimeUnit.NANOSECONDS.toMillis(threadMXBean.getThreadUserTime(threadInfo2.getThreadId()));

			String msg = String.format("[%s] CPU %d ms | USER: %d ms | WAIT %d ms | BLOCKED: %d ms",
					threadInfo2.getThreadName(), cpuTime, userTime, waitedTime, blockedTime);
			System.out.println(msg);
		}

		q.deActivate();

	}

}
