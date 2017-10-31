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

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import common.Active;
import common.ActiveRunnable;
import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.RichTuple;
import common.tuple.Tuple;
import operator.BaseOperator;
import operator.Operator;
import operator.OperatorStatistic;
import operator.Union.UnionOperator;
import operator.aggregate.TimeBasedSingleWindow;
import operator.aggregate.TimeBasedSingleWindowAggregate;
import operator.filter.FilterFunction;
import operator.filter.FilterOperator;
import operator.in2.BaseOperator2In;
import operator.in2.Operator2In;
import operator.in2.Operator2InStatistic;
import operator.in2.join.Predicate;
import operator.in2.join.TimeBasedJoin;
import operator.map.FlatMapFunction;
import operator.map.FlatMapOperator;
import operator.map.MapFunction;
import operator.map.MapOperator;
import operator.router.RouterFunction;
import operator.router.RouterOperator;
import operator.router.RouterStatisticOperator;
import scheduling.Scheduler;
import scheduling.impl.NoopScheduler;
import sink.BaseSink;
import sink.Sink;
import sink.SinkFunction;
import sink.SinkStatistic;
import sink.text.TextSink;
import sink.text.TextSinkFunction;
import source.BaseSource;
import source.Source;
import source.SourceStatistic;
import source.text.TextSource;
import source.text.TextSourceFunction;
import stream.Stream;
import stream.StreamFactory;

public class Query {

	private boolean keepStatistics = false;
	private String statsFolder;
	private boolean autoFlush;

	// TODO: Hashing to ensure uniqueness of streams, sources etc
	// TODO: Implement toString(), hashcode and equals for all entities
	// FIXME: Default StreamFactory for all entities to keep backward compatibility
	private final Map<String, Stream<? extends Tuple>> streams = new HashMap<>();
	private final Map<String, Operator<? extends Tuple, ? extends Tuple>> operators = new HashMap<>();
	private final Map<String, Operator2In<? extends Tuple, ? extends Tuple, ? extends Tuple>> operators2in = new HashMap<>();
	private final Map<String, Source<? extends Tuple>> sources = new HashMap<>();
	private final Map<String, Sink<? extends Tuple>> sinks = new HashMap<>();

	private final List<Thread> threads = new LinkedList<>();
	private final Scheduler scheduler;
	private StreamFactory streamFactory = ConcurrentLinkedListStreamFactory.INSTANCE;

	public Query() {
		this.scheduler = new NoopScheduler();
	}

	public Query(Scheduler scheduler) {
		this.scheduler = scheduler;
	}

	public void activateStatistics(String statisticsFolder, boolean autoFlush) {
		keepStatistics = true;
		this.statsFolder = statisticsFolder;
		this.autoFlush = autoFlush;
		// TODO: Constants
		streamFactory = new ConcurrentLinkedListStreamStatisticFactory(statisticsFolder, "in", "out", autoFlush);
	}

	public void activateStatistics(String statisticsFolder) {
		activateStatistics(statisticsFolder, true);
	}

	public <IN extends Tuple, OUT extends Tuple> Operator<IN, OUT> addOperator(String identifier,
			BaseOperator<IN, OUT> operator) {
		if (keepStatistics) {
			operator = new OperatorStatistic<IN, OUT>(operator, statsFolder + File.separator + identifier + ".proc.csv",
					autoFlush);
		}
		operators.put(operator.getId(), operator);
		return operator;
	}

	public <IN extends RichTuple, OUT extends RichTuple> Operator<IN, OUT> addAggregateOperator(String identifier,
			TimeBasedSingleWindow<IN, OUT> window, long WS, long WA) {

		return addOperator(identifier,
				new TimeBasedSingleWindowAggregate<IN, OUT>(identifier, streamFactory, WS, WA, window));
	}

	public <IN extends Tuple, OUT extends Tuple> Operator<IN, OUT> addMapOperator(String identifier,
			MapFunction<IN, OUT> mapFunction) {
		return addOperator(identifier, new MapOperator<IN, OUT>(identifier, streamFactory, mapFunction));
	}

	public <IN extends Tuple, OUT extends Tuple> Operator<IN, OUT> addMapOperator(String identifier,
			FlatMapFunction<IN, OUT> mapFunction, StreamProducer<IN> inKey, StreamConsumer<OUT> outKey) {
		return addOperator(identifier, new FlatMapOperator<IN, OUT>(identifier, streamFactory, mapFunction));
	}

	public <T extends Tuple> Operator<T, T> addFilterOperator(String identifier, FilterFunction<T> filterF) {
		return addOperator(identifier, new FilterOperator<T>(identifier, streamFactory, filterF));
	}

	public <T extends Tuple> Operator<T, T> addRouterOperator(String identifier, RouterFunction<T> routerF) {
		BaseOperator<T, T> router = null;
		// Notice that the router is a special case which needs a dedicated
		// statistics operator
		if (keepStatistics) {
			router = new RouterStatisticOperator<T>(identifier, streamFactory, routerF,
					statsFolder + File.separator + identifier + ".proc.csv", autoFlush);
		} else {
			router = new RouterOperator<T>(identifier, streamFactory, routerF);
		}
		operators.put(router.getId(), router);
		return router;
	}

	public <T extends Tuple> Operator<T, T> addUnionOperator(String identifier) {

		// Notice that the union is a special case. No processing stats are kept
		// since the union does not process tuples.
		BaseOperator<T, T> union = new UnionOperator<>(identifier, streamFactory);
		operators.put(union.getId(), union);
		return union;
	}

	public <T extends Tuple> Source<T> addSource(BaseSource<T> source) {
		if (keepStatistics) {
			source = new SourceStatistic<T>(source, streamFactory,
					statsFolder + File.separator + source.getId() + ".proc.csv");
		}
		sources.put(source.getId(), source);
		return source;
	}

	public <T extends Tuple> Source<T> addTextSource(String id, String fileName, TextSourceFunction<T> function) {
		return addSource(new TextSource<T>(id, fileName, function));
	}

	public <T extends Tuple> Sink<T> addSink(BaseSink<T> sink) {
		if (keepStatistics) {
			sink = new SinkStatistic<T>(sink, statsFolder + File.separator + sink.getId() + ".proc.csv");
		}
		sinks.put(sink.getId(), sink);
		return sink;
	}

	public <T extends Tuple> Sink<T> addBaseSink(String id, SinkFunction<T> sinkFunction) {
		return addSink(new BaseSink<>(id, streamFactory, sinkFunction));
	}

	public <T extends Tuple> Sink<T> addTextSink(String id, String fileName, TextSinkFunction<T> function,
			StreamProducer<T> streamKey) {
		return addSink(new TextSink<T>(id, fileName, function, true));
	}

	public <T extends Tuple> Sink<T> addTextSink(String id, String fileName, TextSinkFunction<T> function,
			boolean autoFlush) {
		return addSink(new TextSink<T>(id, fileName, function, autoFlush));
	}

	public <OUT extends Tuple, IN extends Tuple, IN2 extends Tuple> Operator2In<IN, IN2, OUT> addOperator2In(
			String identifier, BaseOperator2In<IN, IN2, OUT> operator) {
		if (keepStatistics) {
			operator = new Operator2InStatistic<IN, IN2, OUT>(operator,
					statsFolder + File.separator + identifier + ".proc.csv", autoFlush);
		}
		operators2in.put(operator.getId(), operator);
		return operator;
	}

	public <IN extends RichTuple, IN2 extends RichTuple, OUT extends RichTuple> Operator2In<IN, IN2, OUT> addJoinOperator(
			String identifier, Predicate<IN, IN2, OUT> predicate, long WS) {
		return addOperator2In(identifier, new TimeBasedJoin<IN, IN2, OUT>(identifier, streamFactory, WS, predicate));
	}

	public void activate() {
		System.out.println("*** [Query] Activating...");
		activateTasks(streams.values());
		activateTasks(sinks.values());
		for (ActiveRunnable o : getAllOperators()) {
			o.activate();
		}
		scheduler.addTasks(getAllOperators());
		scheduler.startTasks();
		activateTasks(sources.values());
	}

	public void deActivate() {
		System.out.println("*** [Query] Deactivating...");
		deactivateTasks(sources.values());
		deactivateTasks(getAllOperators());
		deactivateTasks(sinks.values());
		scheduler.stopTasks();
		for (Thread t : threads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		deactivateTasks(streams.values());
	}

	private void activateTasks(Collection<? extends Active> tasks) {
		for (Active task : tasks) {
			task.activate();
			if (task instanceof Runnable) {
				Thread t = new Thread((Runnable) task);
				t.start();
				threads.add(t);
			}
		}
	}

	private void deactivateTasks(Collection<? extends Active> tasks) {
		for (Active task : tasks) {
			task.deActivate();
		}
	}

	private Collection<Operator<?, ?>> getAllOperators() {
		List<Operator<?, ?>> allOperators = new ArrayList<>(operators.values());
		allOperators.addAll(this.operators2in.values());
		return allOperators;
	}
}
