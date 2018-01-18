package queries;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import common.NamedEntity;
import common.util.Util;
import dummy.DummyLatencyLogger;
import dummy.DummyMapFunction;
import dummy.DummyRouterFunction;
import dummy.DummySourceFunction;
import dummy.DummyTuple;
import dummy.RoundRobinTaskPool;
import operator.Operator;
import operator.in2.Operator2In;
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

public class SampleQuery {

	private static class ProcessingRate {
		// Query 1
		static final long I1 = 50;
		static final long A = 40;
		static final long B = 30;
		static final long C = 20;

		// Query 2
		static final long I2 = 140;
		static final long D = 135;
		static final long E = 130;
		static final long F = 125;

		// Query 3
		static final long I3 = 60;
		static final long I4 = 75;
		static final long G = 40;
		static final long H = 40;
		static final long L = 20;

		// Query 4
		static final long I5 = 35;
		static final long M = 30;
		static final long N = 25;

		private ProcessingRate() {
		}
	}

	private static class Selectivity {
		// Query 1
		static final double A = 0.9;
		static final double B = 0.8;
		static final double C = 1.0;

		// Query 2
		static final double D = 0.9;
		static final double E = 0.9;
		static final double F = 0.8;

		// Query 3
		static final double G = 0.9;
		static final double H = 0.9;
		static final double L = 0.5;

		// Query 4
		static final double M = 0.9;
		static final double N = 1.0;

		private Selectivity() {
		}
	}

	private static final Map<String, Double> sMap = new HashMap<String, Double>() {
		private static final long serialVersionUID = 1L;

		{
			put("I1", 1.0);
			put("I2", 1.0);
			put("I3", 1.0);
			put("I4", 1.0);
			put("I5", 1.0);
			put("A", Selectivity.A);
			put("B", Selectivity.B);
			put("C", Selectivity.C);
			put("D", Selectivity.D);
			put("E", Selectivity.E);
			put("F", Selectivity.F);
			put("G", Selectivity.G);
			put("H", Selectivity.H);
			put("L", Selectivity.L);
			put("M", Selectivity.M);
			put("N", Selectivity.N);
		}
	};

	private static final Map<String, Long> rMap = new HashMap<String, Long>() {
		private static final long serialVersionUID = 1L;

		{
			put("I1", ProcessingRate.I1);
			put("I2", ProcessingRate.I2);
			put("I3", ProcessingRate.I3);
			put("I4", ProcessingRate.I4);
			put("I5", ProcessingRate.I5);
			put("A", ProcessingRate.A);
			put("B", ProcessingRate.B);
			put("C", ProcessingRate.C);
			put("D", ProcessingRate.D);
			put("E", ProcessingRate.E);
			put("F", ProcessingRate.F);
			put("G", ProcessingRate.G);
			put("H", ProcessingRate.H);
			put("L", ProcessingRate.L);
			put("M", ProcessingRate.M);
			put("N", ProcessingRate.N);
		}
	};

	private static final String PROPERTY_FILENAME = "liebre.properties";

	public static void main(String[] args) {

		if (args.length != 2) {
			throw new IllegalArgumentException(
					"Program requires two arguments: output folder, simulation duration (seconds)");
		}
		// Configuration Init
		final String statisticsFolder = args[0];
		final long queryDurationMillis = TimeUnit.SECONDS.toMillis(Long.parseLong(args[1]));
		final QueryConfiguration config = new QueryConfiguration(PROPERTY_FILENAME, SampleQuery.class);

		// Query creation
		final Query q;
		final TaskPool<Operator<?, ?>> pool;
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
				pool = new ProbabilisticTaskPool(config.getPriorityMetric(), config.getThreadsNumber(),
						config.getPriorityScalingFactor(), statisticsFolder);
				break;
			default:
				throw new IllegalArgumentException("Unknown TaskPool type!");
			}
			Scheduler scheduler = new ThreadPoolScheduler(config.getThreadsNumber(), config.getSchedulingInterval(),
					TimeUnit.MILLISECONDS, pool);
			q = new Query(scheduler);
		} else { // Otherwise, no scheduler
			q = new Query();
		}

		q.activateStatistics(statisticsFolder);

		// Query Q1
		Source<DummyTuple> i1 = q.addBaseSource("I1", new DummySourceFunction(ProcessingRate.I1));
		Operator<DummyTuple, DummyTuple> A = q.addRouterOperator("A",
				new DummyRouterFunction(Selectivity.A, ProcessingRate.A, Arrays.asList("B", "C")));

		Operator<DummyTuple, DummyTuple> B = q.addMapOperator("B",
				new DummyMapFunction(Selectivity.B, ProcessingRate.B));
		Operator<DummyTuple, DummyTuple> C = q.addMapOperator("C",
				new DummyMapFunction(Selectivity.C, ProcessingRate.C));
		Sink<DummyTuple> o1 = q.addBaseSink("O1",
				new DummyLatencyLogger(statisticsFolder + File.separator + "SINK_" + "O1.latency.csv"));
		Sink<DummyTuple> o2 = q.addBaseSink("O2",
				new DummyLatencyLogger(statisticsFolder + File.separator + "SINK_" + "O2.latency.csv"));

		i1.addOutput(A);
		A.addOutput(B);
		A.addOutput(C);
		B.addOutput(o1);
		C.addOutput(o2);

		// Query Q2
		Source<DummyTuple> i2 = q.addBaseSource("I2", new DummySourceFunction(ProcessingRate.I2));

		Operator<DummyTuple, DummyTuple> D = q.addMapOperator("D",
				new DummyMapFunction(Selectivity.D, ProcessingRate.D));
		Operator<DummyTuple, DummyTuple> E = q.addMapOperator("E",
				new DummyMapFunction(Selectivity.E, ProcessingRate.E));
		Operator<DummyTuple, DummyTuple> F = q.addMapOperator("F",
				new DummyMapFunction(Selectivity.F, ProcessingRate.F));

		Sink<DummyTuple> o3 = q.addBaseSink("O3",
				new DummyLatencyLogger(statisticsFolder + File.separator + "SINK_" + "O3.latency.csv"));

		i2.addOutput(D);
		D.addOutput(E);
		E.addOutput(F);
		F.addOutput(o3);

		// Query Q3

		Source<DummyTuple> i3 = q.addBaseSource("I3", new DummySourceFunction(ProcessingRate.I3));
		Source<DummyTuple> i4 = q.addBaseSource("I4", new DummySourceFunction(ProcessingRate.I4));

		Operator<DummyTuple, DummyTuple> G = q.addMapOperator("G",
				new DummyMapFunction(Selectivity.G, ProcessingRate.G));
		Operator<DummyTuple, DummyTuple> H = q.addMapOperator("H",
				new DummyMapFunction(Selectivity.H, ProcessingRate.H));
		Operator<DummyTuple, DummyTuple> I = q.addUnionOperator("I");
		Operator<DummyTuple, DummyTuple> L = q.addMapOperator("L",
				new DummyMapFunction(Selectivity.L, ProcessingRate.L));

		Sink<DummyTuple> o4 = q.addBaseSink("O4",
				new DummyLatencyLogger(statisticsFolder + File.separator + "SINK_" + "O4.latency.csv"));

		i3.addOutput(G);
		i4.addOutput(H);
		G.addOutput(I);
		H.addOutput(I);
		I.addOutput(L);
		L.addOutput(o4);

		// Query Q4
		Source<DummyTuple> i5 = q.addBaseSource("I5", new DummySourceFunction(ProcessingRate.I5));

		Operator<DummyTuple, DummyTuple> M = q.addMapOperator("M",
				new DummyMapFunction(Selectivity.M, ProcessingRate.M));
		Operator<DummyTuple, DummyTuple> N = q.addMapOperator("N",
				new DummyMapFunction(Selectivity.N, ProcessingRate.N));

		Sink<DummyTuple> o5 = q.addBaseSink("O5",
				new DummyLatencyLogger(statisticsFolder + File.separator + "SINK_" + "O5.latency.csv"));

		i5.addOutput(M);
		M.addOutput(N);
		N.addOutput(o5);

		System.out.format("*** Required Capacity = %.1f threads%n",
				requiredThreads(Arrays.asList(A, B, C, D, E, F, G, H, L, M, N)));
		System.out.format("*** Starting sample query with configuration: %s%n", config);
		System.out.format("*** Statistics folder: %s/%s%n", System.getProperty("user.dir"), statisticsFolder);
		System.out.format("*** Duration: %s seconds%n", args[1]);

		// Start queries and let run for some time
		q.activate();
		Util.sleep(queryDurationMillis);
		q.deActivate();

	}

	private static double requiredThreads(List<Operator<?, ?>> operators) {
		double total = 0;
		for (Operator<?, ?> op : operators) {
			long capacity = rMap.get(op.getId());
			List<Double> inputRates = new ArrayList<>();
			for (NamedEntity prev : op.getPrevious()) {
				if (!rMap.containsKey(prev.getId())) {
					continue;
				}
				long tpt = rMap.get(prev.getId());
				double s = sMap.get(prev.getId());
				// Input Rate = Selectivity * 1/tuples/second
				double inputRate = s / tpt;
				if (Double.isFinite(inputRate)) {
					inputRates.add(inputRate);
				}
			}
			if (inputRates.isEmpty()) {
				continue;
			}
			double inputRate = op instanceof Operator2In ? Collections.min(inputRates) : Collections.max(inputRates);
			total += capacity * inputRate;
		}
		return total;
	}

}
