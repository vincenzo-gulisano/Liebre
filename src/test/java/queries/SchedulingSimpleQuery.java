package queries;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import common.util.Util;
import dummy.DummyLatencyLogger;
import dummy.DummyMapFunction;
import dummy.DummyRouterFunction;
import dummy.DummySourceFunction;
import dummy.DummyTuple;
import operator.Operator;
import operator.PriorityMetric;
import operator.QueueSizePriorityMetric;
import query.Query;
import reports.Report;
import scheduling.Scheduler;
import scheduling.TaskPool;
import scheduling.impl.PriorityTaskPool;
import scheduling.impl.ThreadPoolScheduler;
import sink.Sink;
import source.Source;

public class SchedulingSimpleQuery {
	private static final long SIMULATION_DURATION_MILLIS = 30 * 1000;
	private static final PriorityMetric metric = QueueSizePriorityMetric.INSTANCE;

	private static class Throughput {
		// Query 1
		static final long I1 = 200;
		static final long A = 170;
		static final long B = 100;
		static final long C = 10;

		private Throughput() {
		}
	}

	public static void main(String[] args) {
		TaskPool<Operator<?, ?>> pool = new PriorityTaskPool(metric);
		Scheduler scheduler = new ThreadPoolScheduler(1, 100, TimeUnit.MILLISECONDS, pool);
		Query q = new Query(scheduler);

		// This to store all statistics in the given folder
		q.activateStatistics(args[0]);

		// Query Q1
		Source<DummyTuple> i1 = q.addBaseSource("I1", new DummySourceFunction(Throughput.I1));
		Operator<DummyTuple, DummyTuple> A = q.addRouterOperator("A",
				new DummyRouterFunction(0.9, Throughput.A, Arrays.asList("B", "C")));

		Operator<DummyTuple, DummyTuple> B = q.addMapOperator("B", new DummyMapFunction(1.0, Throughput.B));
		Operator<DummyTuple, DummyTuple> C = q.addMapOperator("C", new DummyMapFunction(1.0, Throughput.C));
		Sink<DummyTuple> o1 = q.addBaseSink("O1", new DummyLatencyLogger(args[0] + File.separator + "O1.latency.csv"));
		Sink<DummyTuple> o2 = q.addBaseSink("O2", new DummyLatencyLogger(args[0] + File.separator + "O2.latency.csv"));

		// TODO: Chaining
		i1.addOutput(A);
		A.addOutput(B);
		A.addOutput(C);
		B.addOutput(o1);
		C.addOutput(o2);

		// Start queries and let run for a time
		System.out.println("Available Processors: " + Runtime.getRuntime().availableProcessors());
		q.activate();
		Util.sleep(SIMULATION_DURATION_MILLIS);
		q.deActivate();

		// Report basic measurements
		Report.reportOutput("latency", "ms", args[0]);
		System.out.println(pool.toString());
	}
}
