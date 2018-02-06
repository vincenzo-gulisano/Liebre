/*  Copyright (C) 2017  Vincenzo Gulisano
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *  
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *  
 *  Contact: Vincenzo Gulisano info@vincenzogulisano.com
 *
 */

package query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import common.Active;
import common.ActiveRunnable;
import common.StreamProducer;
import common.tuple.RichTuple;
import common.tuple.Tuple;
import operator.Operator;
import operator.Union.UnionOperator;
import operator.aggregate.TimeBasedSingleWindow;
import operator.aggregate.TimeBasedSingleWindowAggregate;
import operator.filter.FilterFunction;
import operator.filter.FilterOperator;
import operator.in1.Operator1In;
import operator.in1.Operator1InStatistic;
import operator.in2.Operator2In;
import operator.in2.Operator2InStatistic;
import operator.in2.join.Predicate;
import operator.in2.join.TimeBasedJoin;
import operator.map.FlatMapFunction;
import operator.map.FlatMapOperator;
import operator.map.MapFunction;
import operator.map.MapOperator;
import operator.router.BaseRouterOperator;
import operator.router.RouterFunction;
import operator.router.RouterOperator;
import operator.router.RouterOperatorStatistic;
import scheduling.ActiveThread;
import scheduling.Scheduler;
import scheduling.impl.BasicWorkerThread;
import scheduling.impl.NoopScheduler;
import sink.BaseSink;
import sink.Sink;
import sink.SinkFunction;
import sink.SinkStatistic;
import source.BaseSource;
import source.Source;
import source.SourceFunction;
import source.SourceStatistic;
import stream.Stream;
import stream.StreamFactory;

public class Query {

	private boolean keepStatistics = false;
	private String statsFolder;
	private boolean autoFlush;

	private final Map<String, Operator<? extends Tuple, ? extends Tuple>> operators = new HashMap<>();
	private final Map<String, Operator2In<? extends Tuple, ? extends Tuple, ? extends Tuple>> operators2in = new HashMap<>();
	private final Map<String, Source<? extends Tuple>> sources = new HashMap<>();
	private final Map<String, Sink<? extends Tuple>> sinks = new HashMap<>();

	private final List<ActiveThread> threads = new ArrayList<>();
	private final Scheduler scheduler;
	private StreamFactory streamFactory = BlockingStreamFactory.INSTANCE;

	public Query() {
		this.scheduler = new NoopScheduler();
	}

	public Query(Scheduler scheduler) {
		this.scheduler = scheduler;
	}

	public void activateStatistics(String statisticsFolder, boolean autoFlush) {
		this.keepStatistics = true;
		this.statsFolder = statisticsFolder;
		this.autoFlush = autoFlush;
		streamFactory = new StreamStatisticFactory(streamFactory, statisticsFolder, autoFlush);
		this.scheduler.activateStatistics(statisticsFolder, "");
	}

	public void activateStatistics(String statisticsFolder, String executionId, boolean autoFlush) {
		this.keepStatistics = true;
		this.statsFolder = statisticsFolder;
		this.autoFlush = autoFlush;
		streamFactory = new StreamStatisticFactory(streamFactory, statisticsFolder, autoFlush);
		this.scheduler.activateStatistics(statisticsFolder, executionId);
	}

	public void activateSchedulingStatistics(String statisticsFolder, String executionId) {
		this.scheduler.activateStatistics(statisticsFolder, executionId);
	}

	public void activateStatistics(String statisticsFolder) {
		activateStatistics(statisticsFolder, true);
	}

	public <IN extends Tuple, OUT extends Tuple> Operator<IN, OUT> addOperator(Operator1In<IN, OUT> operator) {
		checkIfExists(operator.getId(), "Operator", operators);
		Operator<IN, OUT> op = operator;
		if (keepStatistics) {
			op = new Operator1InStatistic<IN, OUT>(operator, statsFolder, autoFlush);
		}
		operators.put(op.getId(), op);
		return op;
	}

	public <IN extends RichTuple, OUT extends RichTuple> Operator<IN, OUT> addAggregateOperator(String identifier,
			TimeBasedSingleWindow<IN, OUT> window, long windowSize, long windowSlide) {

		return addOperator(new TimeBasedSingleWindowAggregate<IN, OUT>(identifier, streamFactory, windowSize,
				windowSlide, window));
	}

	public <IN extends Tuple, OUT extends Tuple> Operator<IN, OUT> addMapOperator(String identifier,
			MapFunction<IN, OUT> mapFunction) {
		return addOperator(new MapOperator<IN, OUT>(identifier, streamFactory, mapFunction));
	}

	public <IN extends Tuple, OUT extends Tuple> Operator<IN, OUT> addMapOperator(String identifier,
			FlatMapFunction<IN, OUT> mapFunction) {
		return addOperator(new FlatMapOperator<IN, OUT>(identifier, streamFactory, mapFunction));
	}

	public <T extends Tuple> Operator<T, T> addFilterOperator(String identifier, FilterFunction<T> filterF) {
		return addOperator(new FilterOperator<T>(identifier, streamFactory, filterF));
	}

	public <T extends Tuple> Operator<T, T> addRouterOperator(String identifier, RouterFunction<T> routerF) {
		checkIfExists(identifier, "Operator", operators);
		RouterOperator<T> router = new BaseRouterOperator<T>(identifier, streamFactory, routerF);
		// Notice that the router is a special case which needs a dedicated
		// statistics operator
		if (keepStatistics) {
			router = new RouterOperatorStatistic<T>(router, statsFolder, autoFlush);
		}
		operators.put(router.getId(), router);
		return router;
	}

	public <T extends Tuple> UnionOperator<T> addUnionOperator(String identifier) {
		checkIfExists(identifier, "Operator", operators);
		// Notice that the union is a special case. No processing stats are kept
		// since the union does not process tuples.
		UnionOperator<T> union = new UnionOperator<>(identifier, streamFactory);
		operators.put(union.getId(), union);
		return union;
	}

	public <T extends Tuple> Source<T> addSource(Source<T> source) {
		checkIfExists(source.getId(), "Source", sources);
		Source<T> s = source;
		if (keepStatistics) {
			s = new SourceStatistic<T>(s, streamFactory, statsFolder);
		}
		sources.put(s.getId(), s);
		return s;
	}

	public <T extends Tuple> Source<T> addBaseSource(String id, SourceFunction<T> function) {
		return addSource(new BaseSource<>(id, function));
	}

	public <T extends Tuple> Sink<T> addSink(Sink<T> sink) {
		checkIfExists(sink.getId(), "Sink", sinks);
		Sink<T> s = sink;
		if (keepStatistics) {
			s = new SinkStatistic<T>(sink, statsFolder);
		}
		sinks.put(s.getId(), s);
		return s;
	}

	public <T extends Tuple> Sink<T> addBaseSink(String id, SinkFunction<T> sinkFunction) {
		return addSink(new BaseSink<>(id, streamFactory, sinkFunction));
	}

	public <OUT extends Tuple, IN extends Tuple, IN2 extends Tuple> Operator2In<IN, IN2, OUT> addOperator2In(
			Operator2In<IN, IN2, OUT> operator) {
		checkIfExists(operator.getId(), "Operator", operators2in);
		Operator2In<IN, IN2, OUT> op = operator;
		if (keepStatistics) {
			op = new Operator2InStatistic<IN, IN2, OUT>(operator, statsFolder, autoFlush);
		}
		operators2in.put(op.getId(), op);
		return op;
	}

	public <IN extends RichTuple, IN2 extends RichTuple, OUT extends RichTuple> Operator2In<IN, IN2, OUT> addJoinOperator(
			String identifier, Predicate<IN, IN2, OUT> predicate, long windowSize) {
		return addOperator2In(new TimeBasedJoin<IN, IN2, OUT>(identifier, streamFactory, windowSize, predicate));
	}

	public void activate() {
		System.out.println("*** [Query] Activating...");
		System.out.format("*** [Query] Sinks: %d%n", sinks.size());
		System.out.format("*** [Query] Operators: %d%n", operators.size() + operators2in.size());
		System.out.format("*** [Query] Sources: %d%n", sources.size());
		System.out.format("*** [Query] Streams: %d%n", getAllStreams().size());
		activateTasks(sinks.values());
		for (ActiveRunnable o : getAllOperators()) {
			o.enable();
		}
		scheduler.addTasks(getAllOperators());
		scheduler.startTasks();
		activateTasks(sources.values());
	}

	private Set<Stream<?>> getAllStreams() {
		Set<Stream<?>> streams = new HashSet<>();
		for (Operator<?, ?> op : getAllOperators()) {
			for (StreamProducer<?> prev : op.getPrevious()) {
				streams.add(prev.getOutputStream(op.getId()));
			}
		}
		return streams;
	}

	public void deActivate() {
		System.out.println("*** [Query] Deactivating sinks...");
		deactivateTasks(sinks.values());
		System.out.println("*** [Query] Deactivating operators...");
		deactivateTasks(getAllOperators());
		System.out.println("*** [Query] Deactivating sources...");
		deactivateTasks(sources.values());
		System.out.println("*** [Query] Waiting for threads to terminate...");
		scheduler.stopTasks();
		for (ActiveThread t : threads) {
			try {
				t.disable();
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
			}
		}
		System.out.println("*** [Query] Done!");
	}

	private void activateTasks(Collection<? extends Active> tasks) {
		for (Active task : tasks) {
			task.enable();
			if (task instanceof Runnable) {
				ActiveThread t = new BasicWorkerThread((Runnable) task);
				t.start();
				threads.add(t);
			}
		}
	}

	private void deactivateTasks(Collection<? extends Active> tasks) {
		for (Active task : tasks) {
			task.disable();
		}
	}

	private void checkIfExists(String id, String type, Map<?, ?> map) {
		if (map.containsKey(id)) {
			throw new IllegalArgumentException(String.format("%s with id [%s] already exists in query!", type, id));
		}
	}

	private Collection<Operator<?, ?>> getAllOperators() {
		List<Operator<?, ?>> allOperators = new ArrayList<>(operators.values());
		allOperators.addAll(this.operators2in.values());
		return allOperators;
	}
}
