package queries;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import common.tuple.BaseRichTuple;
import common.util.Util;
import operator.Operator;
import operator.aggregate.BaseTimeBasedSingleWindow;
import operator.aggregate.TimeBasedSingleWindow;
import operator.filter.FilterFunction;
import operator.map.MapFunction;
import query.Query;
import query.QueryConfiguration;
import scheduling.Scheduler;
import scheduling.TaskPool;
import scheduling.impl.ProbabilisticTaskPool;
import scheduling.impl.ThreadPoolScheduler;
import sink.Sink;
import sink.SinkFunction;
import source.Source;
import source.TextSourceFunction;

public class ActivePeriod {

	/*
	 * The fields in the data are: MachineStateID MachineID CommunicationState
	 * Op_stat_red Op_stat_yellow Op_stat_green Op_stat_white Op_stat_off
	 * Op_stat_active Alarm AlarmCode ActiveToolNumber ActivePartProgram
	 * DisruptionID OperationalStateID sDuration DT
	 */

	static class InputTuple extends BaseRichTuple {

		/*
		 * To the best of my understanding, to check active state you need only the
		 * machineID (which can be stored in the key of the BaseRichTuple), the
		 * timestamp (also maintained by the BaseRichTuple) and the color flags.
		 */

		public boolean red;
		public boolean yellow;
		public boolean green;
		public boolean white;

		public InputTuple(long ts, String key, boolean red, boolean yellow, boolean green, boolean white) {
			super(ts, key);
			this.red = red;
			this.yellow = yellow;
			this.green = green;
			this.white = white;
		}

	}

	// One could wonder why you have a dedicated operator to convert the
	// original tuple to the active / inactive one. One can say that is
	// because the original tuple can be fed to multiple operators!

	static class ActiveInactiveTuple extends BaseRichTuple {

		/*
		 * To the best of my understanding, to check active state you need only the
		 * machineID (which can be stored in the key of the BaseRichTuple), the
		 * timestamp (also maintained by the BaseRichTuple) and the color flags.
		 */

		public boolean active;

		// TODO probably good to have this in a cleaner way
		public long previousStateDuration = 0;

		public void setPreviousStateDuration(long previousStateDuration) {
			this.previousStateDuration = previousStateDuration;
		}

		public long getPreviousStateDuration() {
			return previousStateDuration;
		}

		// TODO Check if the conditions can be simplified, should be...

		public ActiveInactiveTuple(long ts, String key, boolean red, boolean yellow, boolean green, boolean white) {
			super(ts, key);

			if (red && !yellow && !green && !white) {
				active = false;
			} else if (!red && yellow && !green && !white) {
				active = true;
			} else if (!red && !yellow && green && !white) {
				active = false;
			} else if (!red && !yellow && !green && white) {
				active = true;
			} else if (red && !yellow && !green && white) {
				active = false;
			} else if (red && !yellow && green && !white) {
				active = false;
			} else if (red && yellow && !green && !white) {
				active = false;
			} else if (red && !yellow && green && white) {
				active = false;
			} else if (red && yellow && !green && white) {
				active = false;
			} else if (red && yellow && green && !white) {
				active = false;
			} else if (!red && yellow && !green && white) {
				active = false;
			} else if (red && yellow && green && white) {
				active = true;
			} else if (!red && yellow && green && !white) {
				active = false;
			} else if (!red && yellow && green && white) {
				active = false;
			} else if (!red && !yellow && green && white) {
				active = true;
			} else if (red && yellow && green && white) {
				active = false;
			} else {
				active = true;
			}
		}

	}

	static class InfoTuple extends BaseRichTuple {

		public long count;
		// public double min;
		// public double max;
		public double average;
		public double confidenceInterval;

		public InfoTuple(long ts, String key, long count, /*
															 * double min, double max,
															 */double average, double confidenceInterval) {
			super(ts, key);
			this.count = count;
			// this.min = min;
			// this.max = max;
			this.average = average;
			this.confidenceInterval = confidenceInterval;
		}

		public String toString() {
			return getTimestamp() + "," + getKey() + "," + count + "," + df2.format(this.average) + "+-"
					+ df2.format(this.confidenceInterval);
		}

	}

	static class Win extends BaseTimeBasedSingleWindow<ActiveInactiveTuple, InfoTuple> {

		private double count = 0;
		private double sum = 0;
		private double squareDifferenceSum = 0;

		// private double min = Double.MAX_VALUE;
		// private double max = Double.MIN_VALUE;

		@Override
		public void add(ActiveInactiveTuple t) {
			squareDifferenceSum += (t.previousStateDuration - sum / count)
					* (t.previousStateDuration - (sum + t.previousStateDuration) / (count + 1));
			count++;
			sum += t.getPreviousStateDuration();
		}

		@Override
		public void remove(ActiveInactiveTuple t) {
			count--;
			sum -= t.getPreviousStateDuration();
		}

		@Override
		public InfoTuple getAggregatedResult() {
			// Just some check
			assert (sum <= WS);
			return new InfoTuple(startTimestamp, key, (long) count, sum / count,
					1.96 * (Math.sqrt(squareDifferenceSum / count)) / Math.sqrt(count));
		}

		@Override
		public TimeBasedSingleWindow<ActiveInactiveTuple, InfoTuple> factory() {
			return new Win();
		}

	}

	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	static DecimalFormat df2 = new DecimalFormat(".###");
	static long WS = 24 * 3600 * 1000;
	static long WA = 24 * 3600 * 1000;

	private static final String PROPERTY_FILENAME = "liebre.properties";

	public static void main(String[] args) {

		if (args.length != 3) {
			throw new IllegalArgumentException(
					"Program requires 3 arguments: inputFile, stats folder, simulation duration (minutes)");
		}
		// Configuration Init
		final String inputFile = args[0];
		final String statisticsFolder = args[1];
		final long queryDurationMillis = TimeUnit.MINUTES.toMillis(Long.parseLong(args[2]));
		final QueryConfiguration config = new QueryConfiguration(PROPERTY_FILENAME, SampleQuery.class);

		// Query creation
		Query q;
		if (config.isSchedulingEnabled()) { // If scheduling enabled, configure
			TaskPool<Operator<?, ?>> pool = new ProbabilisticTaskPool(config.getPriorityMetric(),
					config.getThreadsNumber(), config.getPriorityScalingFactor());
			Scheduler scheduler = new ThreadPoolScheduler(config.getThreadsNumber(), config.getSchedulingInterval(),
					TimeUnit.MILLISECONDS, pool);
			q = new Query(scheduler);
		} else { // Otherwise, no scheduler
			q = new Query();
		}

		q.activateStatistics(statisticsFolder);

		Source<InputTuple> source = q.addBaseSource("source", new TextSourceFunction<InputTuple>(inputFile) {

			private int lineCounter = 0;

			public InputTuple getNext(String line) {

				lineCounter++;

				// Skip the first line
				if (lineCounter >= 2) {

					String[] tokens = line.split(",");
					InputTuple t = null;
					try {
						t = new InputTuple(sdf.parse(line.split(",")[16]).getTime(), tokens[1],
								tokens[3].equals("0") ? false : tokens[3].equals("1") ? true : null,
								tokens[4].equals("0") ? false : tokens[4].equals("1") ? true : null,
								tokens[5].equals("0") ? false : tokens[5].equals("1") ? true : null,
								tokens[6].equals("0") ? false : tokens[6].equals("1") ? true : null);
					} catch (ParseException e) {
						e.printStackTrace();
					}

					return t;

				}

				return null;
			}
		});

		Operator<InputTuple, ActiveInactiveTuple> activeInactive = q.addMapOperator("active_inactive",
				new MapFunction<InputTuple, ActiveInactiveTuple>() {

					public ActiveInactiveTuple map(InputTuple t) {
						return new ActiveInactiveTuple(t.getTimestamp(), t.getKey(), t.red, t.yellow, t.green, t.white);
					}
				});

		Operator<ActiveInactiveTuple, ActiveInactiveTuple> activeInactiveChange = q
				.addFilterOperator("active_inactive_change", new FilterFunction<ActiveInactiveTuple>() {

					HashMap<String, ActiveInactiveTuple> machineState = new HashMap<String, ActiveInactiveTuple>();

					public boolean forward(ActiveInactiveTuple t) {
						boolean forward = false;
						if (!machineState.containsKey(t.getKey())) {
							machineState.put(t.getKey(), t);
						} else if (machineState.get(t.getKey()).active != t.active) {
							t.setPreviousStateDuration(t.getTimestamp() - machineState.get(t.getKey()).getTimestamp());
							machineState.put(t.getKey(), t);
							forward = true;
						}
						return forward;
					}
				});

		// Notice that we want the average of the active periods, which
		// are carried by the "inactive tuples"
		Operator<ActiveInactiveTuple, ActiveInactiveTuple> shiftToInactive = q.addFilterOperator("shiftToInactive",
				new FilterFunction<ActiveInactiveTuple>() {

					public boolean forward(ActiveInactiveTuple arg0) {
						return !arg0.active;
					}
				});

		Operator<ActiveInactiveTuple, InfoTuple> average = q.addAggregateOperator("average", new Win(), WS, WA);

		Sink<InfoTuple> sink = q.addBaseSink("sink", new SinkFunction<InfoTuple>() {

			@Override
			public void processTuple(InfoTuple t) {
				System.out.println(t);
			}
		});

		source.addOutput(activeInactive);
		activeInactive.addOutput(activeInactiveChange);
		activeInactiveChange.addOutput(shiftToInactive);
		shiftToInactive.addOutput(average);
		average.addOutput(sink);

		System.out.format("*** Starting sample query with configuration: %s%n", config);
		System.out.format("*** Statistics folder: %s/%s%n", System.getProperty("user.dir"), statisticsFolder);
		System.out.format("*** Duration: %s minutes%n", args[2]);
		// Run query
		q.activate();
		Util.sleep(queryDurationMillis);
		q.deActivate();

	}
}