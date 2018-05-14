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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import operator.BaseOperator;
import operator.Operator;
import operator.OperatorKey;
import operator.OperatorStatistic;
import operator.Union.UnionOperator;
import operator.aggregate.TimeBasedSingleWindow;
import operator.aggregate.TimeBasedSingleWindowAggregate;
import operator.filter.FilterFunction;
import operator.filter.FilterOperator;
import operator.map.FlatMapFunction;
import operator.map.FlatMapOperator;
import operator.map.MapFunction;
import operator.map.MapOperator;
import operator.router.RouterFunction;
import operator.router.RouterOperator;
import operator.router.RouterStatisticOperator;
import operator2in.BaseOperator2In;
import operator2in.Operator2In;
import operator2in.Operator2InKey;
import operator2in.Operator2InStatistic;
import operator2in.join.Predicate;
import operator2in.join.TimeBasedJoin;
import sink.BaseSink;
import sink.Sink;
import sink.SinkKey;
import sink.SinkStatistic;
import sink.text.TextSink;
import sink.text.TextSinkFunction;
import source.BaseSource;
import source.Source;
import source.SourceKey;
import source.SourceStatistic;
import source.text.TextSource;
import source.text.TextSourceFunction;
import stream.ConcurrentLinkedListStream;
import stream.Stream;
import stream.StreamKey;
import stream.StreamStatistic;
import tuple.RichTuple;
import tuple.Tuple;

public class Query {

	private boolean keepStatistics = false;
	private String statsFolder;
	private boolean autoFlush;

	private final Map<StreamKey<? extends Tuple>, Stream<? extends Tuple>> streams = new HashMap<StreamKey<? extends Tuple>, Stream<? extends Tuple>>();
	private final Map<OperatorKey<? extends Tuple, ? extends Tuple>, Operator<? extends Tuple, ? extends Tuple>> operators = new HashMap<OperatorKey<? extends Tuple, ? extends Tuple>, Operator<? extends Tuple, ? extends Tuple>>();
	private final Map<Operator2InKey<? extends Tuple, ? extends Tuple, ? extends Tuple>, Operator2In<? extends Tuple, ? extends Tuple, ? extends Tuple>> operators2in = new HashMap<Operator2InKey<? extends Tuple, ? extends Tuple, ? extends Tuple>, Operator2In<? extends Tuple, ? extends Tuple, ? extends Tuple>>();
	private final Map<SourceKey<? extends Tuple>, Source<? extends Tuple>> sources = new HashMap<SourceKey<? extends Tuple>, Source<? extends Tuple>>();
	private final Map<SinkKey<? extends Tuple>, Sink<? extends Tuple>> sinks = new HashMap<SinkKey<? extends Tuple>, Sink<? extends Tuple>>();

	private List<Thread> threads;

	public Query() {
		threads = new LinkedList<Thread>();
	}

	public void activateStatistics(String statisticsFolder, boolean autoFlush) {
		keepStatistics = true;
		this.statsFolder = statisticsFolder;
		this.autoFlush = autoFlush;
	}

	public void activateStatistics(String statisticsFolder) {
		keepStatistics = true;
		this.statsFolder = statisticsFolder;
		this.autoFlush = true;
	}

	public <T extends Tuple> StreamKey<T> addStream(String identifier,
			Class<T> type) {
		StreamKey<T> key = new StreamKey<>(identifier, type);
		Stream<T> stream = new ConcurrentLinkedListStream<T>();
		if (keepStatistics) {
			stream = new StreamStatistic<T>(stream, statsFolder
					+ File.separator + identifier + ".in.csv", statsFolder
					+ File.separator + identifier + ".out.csv", autoFlush);
		}
		streams.put(key, stream);
		return key;
	}

	@SuppressWarnings("unchecked")
	public <T1 extends Tuple, T2 extends Tuple> Operator<T1, T2> addOperator(
			String identifier, BaseOperator<T1, T2> operator,
			StreamKey<T1> inKey, StreamKey<T2> outKey) {
		OperatorKey<T1, T2> key = new OperatorKey<T1, T2>(identifier,
				inKey.type, outKey.type);
		if (keepStatistics) {
			operator = new OperatorStatistic<T1, T2>(operator, statsFolder
					+ File.separator + identifier + ".proc.csv", autoFlush);
		}
		operator.registerIn(inKey.identifier, (Stream<T1>) streams.get(inKey));
		operator.registerOut(outKey.identifier,
				(Stream<T2>) streams.get(outKey));
		operators.put(key, operator);
		return operator;
	}

	public <T1 extends RichTuple, T2 extends RichTuple> Operator<T1, T2> addAggregateOperator(
			String identifier, TimeBasedSingleWindow<T1, T2> window, long WS,
			long WA, StreamKey<T1> inKey, StreamKey<T2> outKey) {

		return addOperator(identifier,
				new TimeBasedSingleWindowAggregate<T1, T2>(WS, WA, window),
				inKey, outKey);
	}

	public <T1 extends Tuple, T2 extends Tuple> Operator<T1, T2> addMapOperator(
			String identifier, MapFunction<T1, T2> mapFunction,
			StreamKey<T1> inKey, StreamKey<T2> outKey) {
		return addOperator(identifier, new MapOperator<T1, T2>(mapFunction),
				inKey, outKey);
	}

	public <T1 extends Tuple, T2 extends Tuple> Operator<T1, T2> addMapOperator(
			String identifier, FlatMapFunction<T1, T2> mapFunction,
			StreamKey<T1> inKey, StreamKey<T2> outKey) {
		return addOperator(identifier,
				new FlatMapOperator<T1, T2>(mapFunction), inKey, outKey);
	}

	public <T extends Tuple> Operator<T, T> addFilterOperator(
			String identifier, FilterFunction<T> filterF, StreamKey<T> inKey,
			StreamKey<T> outKey) {
		return addOperator(identifier, new FilterOperator<T>(filterF), inKey,
				outKey);
	}

	@SuppressWarnings("unchecked")
	public <T extends Tuple> Operator<T, T> addRouterOperator(
			String identifier, RouterFunction<T> routerF, StreamKey<T> inKey,
			List<StreamKey<T>> outKeys) {
		OperatorKey<T, T> key = new OperatorKey<T, T>(identifier, inKey.type,
				outKeys.get(0).type);
		BaseOperator<T, T> router = null;
		// Notice that the router is a special case which needs a dedicated
		// statistics operator
		if (keepStatistics) {
			router = new RouterStatisticOperator<T>(routerF, statsFolder
					+ File.separator + identifier + ".proc.csv", autoFlush);
		} else {
			router = new RouterOperator<T>(routerF);
		}
		router.registerIn(inKey.identifier, (Stream<T>) streams.get(inKey));
		for (StreamKey<T> outKey : outKeys)
			router.registerOut(outKey.identifier,
					(Stream<T>) streams.get(outKey));
		operators.put(key, router);
		return router;
	}

	@SuppressWarnings("unchecked")
	public <T extends Tuple> Operator<T, T> addUnionOperator(String identifier,
			List<StreamKey<T>> insKeys, StreamKey<T> outKey) {

		return addUnionOperator(identifier, new UnionOperator<>(), insKeys, outKey);
	}

	@SuppressWarnings("unchecked")
	public <T extends Tuple> Operator<T, T> addUnionOperator(String identifier, UnionOperator<T> union,
			List<StreamKey<T>> insKeys, StreamKey<T> outKey) {

		OperatorKey<T, T> key = new OperatorKey<T, T>(identifier,
				insKeys.get(0).type, outKey.type);
		// Notice that the union is a special case. No processing stats are kept
		// since the union does not process tuples.
		union.registerOut(outKey.identifier, (Stream<T>) streams.get(outKey));
		for (StreamKey<T> inKey : insKeys)
			union.registerIn(inKey.identifier, (Stream<T>) streams.get(inKey));
		operators.put(key, union);
		return union;
	}

	@SuppressWarnings("unchecked")
	public <T extends Tuple> SourceKey<T> addSource(String identifier,
			BaseSource<T> source, StreamKey<T> outKey) {
		SourceKey<T> key = new SourceKey<T>(identifier, outKey.type);
		if (keepStatistics) {
			source = new SourceStatistic<T>(source, statsFolder
					+ File.separator + identifier + ".proc.csv");
		}
		source.registerOut((Stream<T>) streams.get(outKey));
		sources.put(key, source);
		return key;
	}

	public <T extends Tuple> SourceKey<T> addTextSource(String identifier,
			String fileName, TextSourceFunction<T> function, StreamKey<T> outKey) {
		return addSource(identifier, new TextSource<T>(fileName, function),
				outKey);
	}

	@SuppressWarnings("unchecked")
	public <T extends Tuple> SinkKey<T> addSink(String identifier,
			BaseSink<T> sink, StreamKey<T> streamKey) {
		SinkKey<T> key = new SinkKey<T>(identifier, streamKey.type);
		if (keepStatistics) {
			sink = new SinkStatistic<T>(sink, statsFolder + File.separator
					+ identifier + ".proc.csv");
		}
		sink.registerIn((Stream<T>) streams.get(streamKey));
		sinks.put(key, sink);
		return key;
	}

	public <T extends Tuple> SinkKey<T> addTextSink(String identifier,
			String fileName, TextSinkFunction<T> function,
			StreamKey<T> streamKey) {
		return addSink(identifier, new TextSink<T>(fileName, function, true),
				streamKey);
	}

	public <T extends Tuple> SinkKey<T> addTextSink(String identifier,
			String fileName, TextSinkFunction<T> function, boolean autoFlush,
			StreamKey<T> streamKey) {
		return addSink(identifier, new TextSink<T>(fileName, function,
				autoFlush), streamKey);
	}

	@SuppressWarnings("unchecked")
	public <T1 extends Tuple, T2 extends Tuple, T3 extends Tuple> Operator2In<T1, T2, T3> addOperator2In(
			String identifier, BaseOperator2In<T1, T2, T3> operator,
			StreamKey<T1> in1Key, StreamKey<T2> in2Key, StreamKey<T3> outKey) {
		Operator2InKey<T1, T2, T3> key = new Operator2InKey<T1, T2, T3>(
				identifier, in1Key.type, in2Key.type, outKey.type);
		if (keepStatistics) {
			operator = new Operator2InStatistic<T1, T2, T3>(operator,
					statsFolder + File.separator + identifier + ".proc.csv",
					autoFlush);
		}
		operator.registerIn1(in1Key.identifier,
				(Stream<T1>) streams.get(in1Key));
		operator.registerIn2(in2Key.identifier,
				(Stream<T2>) streams.get(in2Key));
		operator.registerOut(outKey.identifier,
				(Stream<T3>) streams.get(outKey));
		operators2in.put(key, operator);
		return operator;
	}

	public <T1 extends RichTuple, T2 extends RichTuple, T3 extends RichTuple> Operator2In<T1, T2, T3> addJoinOperator(
			String identifier, Predicate<T1, T2, T3> predicate, long WS,
			StreamKey<T1> in1Key, StreamKey<T2> in2Key, StreamKey<T3> outKey) {
		return addOperator2In(identifier, new TimeBasedJoin<T1, T2, T3>(WS,
				predicate), in1Key, in2Key, outKey);
	}

	public void disableBackoff(StreamKey<?> streamKey) {
		System.out.format("Disabing stream backoff for %s%n", streamKey.identifier);
		streams.get(streamKey).disableBackoff();
	}

	public void activate() {
		for (Stream<? extends Tuple> s : streams.values()) {
			s.activate();
		}
		for (Sink<? extends Tuple> s : sinks.values()) {
			s.activate();
			Thread t = new Thread(s);
			t.start();
			threads.add(t);
		}
		for (Operator<? extends Tuple, ? extends Tuple> o : operators.values()) {
			o.activate();
			Thread t = new Thread(o);
			t.start();
			threads.add(t);
		}
		for (Operator2In<? extends Tuple, ? extends Tuple, ? extends Tuple> o : operators2in
				.values()) {
			o.activate();
			Thread t = new Thread(o);
			t.start();
			threads.add(t);
		}
		for (Source<? extends Tuple> s : sources.values()) {
			s.activate();
			Thread t = new Thread(s);
			t.start();
			threads.add(t);
		}
	}

	public void deActivate() {
		for (Source<? extends Tuple> s : sources.values()) {
			s.deActivate();
		}
		for (Operator<? extends Tuple, ? extends Tuple> o : operators.values()) {
			o.deActivate();
		}
		for (Operator2In<? extends Tuple, ? extends Tuple, ? extends Tuple> o : operators2in
				.values()) {
			o.deActivate();
		}
		for (Sink<? extends Tuple> s : sinks.values()) {
			s.deActivate();
		}
		for (Thread t : threads) {
			try {
				t.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		for (Stream<? extends Tuple> s : streams.values()) {
			s.deActivate();
		}
	}
}
