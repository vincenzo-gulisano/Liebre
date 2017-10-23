package queries;

import java.io.File;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

import common.util.Util;
import dummy.DummyLatencyLogger;
import dummy.DummyMapFunction;
import dummy.DummyRouterFunction;
import dummy.DummyTuple;
import dummy.FifoTaskPool;
import query.Query;
import reports.Report;
import scheduling.Scheduler;
import scheduling.impl.ThreadPoolScheduler;
import source.BaseSource;
import stream.StreamKey;

public class SampleQuery {

	private static final long SIMULATION_DURATION_MILLIS = 30000;

	private static class Throughput {
		// Query 1
		static final long I1 = 50;
		static final long A = 50;
		static final long B = 30;
		static final long C = 250;

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
		static final long L = 200;

		// Query 4
		static final long I5 = 35;
		static final long M = 50;
		static final long N = 5000;

		private Throughput() {
		}
	}

	public static void main(String[] args) {

		Scheduler scheduler = new ThreadPoolScheduler(12, 50, TimeUnit.MILLISECONDS, new FifoTaskPool());
		Query q = new Query(scheduler);

		// This to store all statistics in the given folder
		q.activateStatistics(args[0]);

		// Query Q1

		StreamKey<DummyTuple> A_i1Key = q.addStream("A_i1", DummyTuple.class);
		StreamKey<DummyTuple> B_i1Key = q.addStream("B_i1", DummyTuple.class);
		StreamKey<DummyTuple> C_i1Key = q.addStream("C_i1", DummyTuple.class);
		StreamKey<DummyTuple> B_o1Key = q.addStream("B_o1", DummyTuple.class);
		StreamKey<DummyTuple> C_o1Key = q.addStream("C_o1", DummyTuple.class);

		LinkedList<StreamKey<DummyTuple>> outs = new LinkedList<StreamKey<DummyTuple>>();
		outs.add(B_i1Key);
		outs.add(C_i1Key);

		q.addSource("I1", new BaseSource<DummyTuple>() {
			@Override
			public DummyTuple getNextTuple() {
				Util.sleep(Throughput.I1);
				return new DummyTuple(System.nanoTime());
			}
		}, A_i1Key);

		q.addRouterOperator("A", new DummyRouterFunction(0.9, Throughput.A, Arrays.asList("B_i1", "C_i1")), A_i1Key,
				outs);
		q.addMapOperator("B", new DummyMapFunction(0.8, Throughput.B), B_i1Key, B_o1Key);
		q.addMapOperator("C", new DummyMapFunction(1, Throughput.C), C_i1Key, C_o1Key);

		q.addSink("O1", new DummyLatencyLogger(args[0] + File.separator + "O1.latency.csv"), B_o1Key);

		q.addSink("O2", new DummyLatencyLogger(args[0] + File.separator + "O2.latency.csv"), C_o1Key);

		// Query Q2

		StreamKey<DummyTuple> D_i1Key = q.addStream("D_i1", DummyTuple.class);
		StreamKey<DummyTuple> E_i1Key = q.addStream("E_i1", DummyTuple.class);
		StreamKey<DummyTuple> F_i1Key = q.addStream("F_i1", DummyTuple.class);
		StreamKey<DummyTuple> F_o1Key = q.addStream("F_o1", DummyTuple.class);

		q.addSource("I2", new BaseSource<DummyTuple>() {
			@Override
			public DummyTuple getNextTuple() {
				Util.sleep(Throughput.I2);
				return new DummyTuple(System.nanoTime());
			}
		}, D_i1Key);

		q.addMapOperator("D", new DummyMapFunction(0.9, Throughput.D), D_i1Key, E_i1Key);
		q.addMapOperator("E", new DummyMapFunction(1, Throughput.E), E_i1Key, F_i1Key);
		q.addMapOperator("F", new DummyMapFunction(1, Throughput.F), F_i1Key, F_o1Key);

		q.addSink("O3", new DummyLatencyLogger(args[0] + File.separator + "O3.latency.csv"), F_o1Key);

		// Query Q3

		StreamKey<DummyTuple> G_i1Key = q.addStream("G_i1", DummyTuple.class);
		StreamKey<DummyTuple> H_i1Key = q.addStream("H_i1", DummyTuple.class);
		StreamKey<DummyTuple> I_i1Key = q.addStream("I_i1", DummyTuple.class);
		StreamKey<DummyTuple> I_i2Key = q.addStream("I_i2", DummyTuple.class);
		StreamKey<DummyTuple> L_i1Key = q.addStream("L_i1", DummyTuple.class);
		StreamKey<DummyTuple> L_o1Key = q.addStream("L_o1", DummyTuple.class);

		q.addSource("I3", new BaseSource<DummyTuple>() {
			@Override
			public DummyTuple getNextTuple() {
				Util.sleep(Throughput.I3);
				return new DummyTuple(System.nanoTime());
			}
		}, G_i1Key);

		q.addSource("I4", new BaseSource<DummyTuple>() {
			@Override
			public DummyTuple getNextTuple() {
				Util.sleep(Throughput.I4);
				return new DummyTuple(System.nanoTime());
			}
		}, H_i1Key);

		LinkedList<StreamKey<DummyTuple>> ins = new LinkedList<StreamKey<DummyTuple>>();
		ins.add(I_i1Key);
		ins.add(I_i2Key);

		q.addMapOperator("G", new DummyMapFunction(0.9, Throughput.G), G_i1Key, I_i1Key);
		q.addMapOperator("H", new DummyMapFunction(0.9, Throughput.H), H_i1Key, I_i2Key);
		q.addUnionOperator("I", ins, L_i1Key);
		q.addMapOperator("L", new DummyMapFunction(0.1, Throughput.L), L_i1Key, L_o1Key);

		q.addSink("O4", new DummyLatencyLogger(args[0] + File.separator + "O4.latency.csv"), L_o1Key);

		// Query Q4

		StreamKey<DummyTuple> M_i1Key = q.addStream("M_i1", DummyTuple.class);
		StreamKey<DummyTuple> N_i1Key = q.addStream("N_i1", DummyTuple.class);
		StreamKey<DummyTuple> N_o1Key = q.addStream("N_o1", DummyTuple.class);

		q.addSource("I5", new BaseSource<DummyTuple>() {
			@Override
			public DummyTuple getNextTuple() {
				Util.sleep(Throughput.I5);
				return new DummyTuple(System.nanoTime());
			}
		}, M_i1Key);

		q.addMapOperator("M", new DummyMapFunction(1, Throughput.M), M_i1Key, N_i1Key);
		q.addMapOperator("N", new DummyMapFunction(0.1, Throughput.N), N_i1Key, N_o1Key);

		q.addSink("O5", new DummyLatencyLogger(args[0] + File.separator + "O5.latency.csv"), N_o1Key);

		// Start queries and let run for a time
		q.activate();
		Util.sleep(SIMULATION_DURATION_MILLIS);
		q.deActivate();

		// Report basic measurements
		Report.reportOutput("latency", "ms", args[0]);
		Report.reportOutput("proc", "tuples/second", args[0]);

	}

}
