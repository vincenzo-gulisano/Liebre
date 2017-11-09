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
import operator.StimulousPriorityMetric;
import query.Query;
import scheduling.Scheduler;
import scheduling.TaskPool;
import scheduling.impl.PriorityTaskPool;
import scheduling.impl.ThreadPoolScheduler;
import sink.Sink;
import source.Source;

public class SampleQuery {

	private static final long SIMULATION_DURATION_MILLIS = 4 * 60 * 1000;
	private static final PriorityMetric metric = StimulousPriorityMetric.INSTANCE;

	private static class Throughput {
		// Query 1
		static final long I1 = 50;
		static final long A = 40;
		static final long B = 30;
		static final long C = 20;

		// Query 2
		static final long I2 = 140;
		static final long D = 140;
		static final long E = 140;
		static final long F = 140;

		// Query 3
		static final long I3 = 60;
		static final long I4 = 75;
		static final long G = 40;
		static final long H = 40;
		static final long L = 20;

		// Query 4
		static final long I5 = 35;
		static final long M = 20;
		static final long N = 10;

		private Throughput() {
		}
	}

	public static void main(String[] args) {

		TaskPool<Operator<?, ?>> pool = new PriorityTaskPool(metric);
		Scheduler scheduler = new ThreadPoolScheduler(8, 40, TimeUnit.MILLISECONDS, pool);
		Query q = new Query(scheduler);

		// This to store all statistics in the given folder
		q.activateStatistics(args[0]);

		// Query Q1
		Source<DummyTuple> i1 = q.addBaseSource("I1", new DummySourceFunction(Throughput.I1));
		Operator<DummyTuple, DummyTuple> A = q.addRouterOperator("A",
				new DummyRouterFunction(0.9, Throughput.A, Arrays.asList("B", "C")));

		Operator<DummyTuple, DummyTuple> B = q.addMapOperator("B", new DummyMapFunction(0.8, Throughput.B));
		Operator<DummyTuple, DummyTuple> C = q.addMapOperator("C", new DummyMapFunction(1.0, Throughput.C));
		Sink<DummyTuple> o1 = q.addBaseSink("O1", new DummyLatencyLogger(args[0] + File.separator + "O1.latency.csv"));
		Sink<DummyTuple> o2 = q.addBaseSink("O2", new DummyLatencyLogger(args[0] + File.separator + "O2.latency.csv"));

		// TODO: Chaining
		i1.addOutput(A);
		A.addOutput(B);
		A.addOutput(C);
		B.addOutput(o1);
		C.addOutput(o2);

		// Query Q2
		Source<DummyTuple> i2 = q.addBaseSource("I2", new DummySourceFunction(Throughput.I2));

		Operator<DummyTuple, DummyTuple> D = q.addMapOperator("D", new DummyMapFunction(0.9, Throughput.D));
		Operator<DummyTuple, DummyTuple> E = q.addMapOperator("E", new DummyMapFunction(1, Throughput.E));
		Operator<DummyTuple, DummyTuple> F = q.addMapOperator("F", new DummyMapFunction(1, Throughput.F));

		Sink<DummyTuple> o3 = q.addBaseSink("O3", new DummyLatencyLogger(args[0] + File.separator + "O3.latency.csv"));

		i2.addOutput(D);
		D.addOutput(E);
		E.addOutput(F);
		F.addOutput(o3);

		// Query Q3

		Source<DummyTuple> i3 = q.addBaseSource("I3", new DummySourceFunction(Throughput.I3));
		Source<DummyTuple> i4 = q.addBaseSource("I4", new DummySourceFunction(Throughput.I4));

		Operator<DummyTuple, DummyTuple> G = q.addMapOperator("G", new DummyMapFunction(0.9, Throughput.G));
		Operator<DummyTuple, DummyTuple> H = q.addMapOperator("H", new DummyMapFunction(0.9, Throughput.H));
		Operator<DummyTuple, DummyTuple> I = q.addUnionOperator("I");
		Operator<DummyTuple, DummyTuple> L = q.addMapOperator("L", new DummyMapFunction(0.5, Throughput.L));

		Sink<DummyTuple> o4 = q.addBaseSink("O4", new DummyLatencyLogger(args[0] + File.separator + "O4.latency.csv"));

		i3.addOutput(G);
		i4.addOutput(H);
		G.addOutput(I);
		H.addOutput(I);
		I.addOutput(L);
		L.addOutput(o4);

		// Query Q4
		Source<DummyTuple> i5 = q.addBaseSource("I5", new DummySourceFunction(Throughput.I5));

		Operator<DummyTuple, DummyTuple> M = q.addMapOperator("M", new DummyMapFunction(0.9, Throughput.M));
		Operator<DummyTuple, DummyTuple> N = q.addMapOperator("N", new DummyMapFunction(1, Throughput.N));

		Sink<DummyTuple> o5 = q.addBaseSink("O5", new DummyLatencyLogger(args[0] + File.separator + "O5.latency.csv"));

		i5.addOutput(M);
		M.addOutput(N);
		N.addOutput(o5);

		// Start queries and let run for a time
		System.out.println("Available Processors: " + Runtime.getRuntime().availableProcessors());
		q.activate();
		Util.sleep(SIMULATION_DURATION_MILLIS);
		q.deActivate();

		System.out.println(pool.toString());
	}

}
