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
import operator.aggregate.TimeBasedSingleWindow;
import operator.aggregate.TimeBasedSingleWindowAggregate;
import operator.filter.FilterFunction;
import operator.filter.FilterOperator;
import operator.map.MapFunction;
import operator.map.MapOperator;
import operator2in.BaseOperator2In;
import operator2in.Operator2In;
import operator2in.Operator2InKey;
import operator2in.Operator2InStatistic;
import operator2in.join.Predicate;
import operator2in.join.TimeBasedJoin;
import sink.Sink;
import sink.SinkKey;
import sink.text.TextSink;
import sink.text.TextSinkFunction;
import source.Source;
import source.SourceKey;
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
		StreamKey<T> key = new StreamKey<>("in", type);
		Stream<T> stream = null;
		if (keepStatistics) {
			stream = new StreamStatistic<T>(
					new ConcurrentLinkedListStream<T>(), statsFolder
							+ File.separator + identifier + ".in.csv",
					statsFolder + File.separator + identifier + ".out.csv",
					autoFlush);
		} else {
			stream = new ConcurrentLinkedListStream<T>();
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
		Operator<T1, T2> op = null;
		if (keepStatistics) {
			op = new OperatorStatistic<T1, T2>(operator, statsFolder
					+ File.separator + identifier + ".proc.csv", autoFlush);
		} else {
			op = operator;
		}
		op.registerIn((Stream<T1>) streams.get(inKey));
		op.registerOut((Stream<T2>) streams.get(outKey));
		operators.put(key, op);
		return op;
	}

	@SuppressWarnings("unchecked")
	public <T1 extends RichTuple, T2 extends RichTuple> Operator<T1, T2> addAggregateOperator(
			String identifier, TimeBasedSingleWindow<T1, T2> window, long WS,
			long WA, StreamKey<T1> inKey, StreamKey<T2> outKey) {
		OperatorKey<T1, T2> key = new OperatorKey<T1, T2>(identifier,
				inKey.type, outKey.type);
		Operator<T1, T2> agg = null;
		if (keepStatistics) {
			agg = new OperatorStatistic<T1, T2>(
					new TimeBasedSingleWindowAggregate<T1, T2>(WS, WA, window),
					statsFolder + File.separator + identifier + ".proc.csv",
					autoFlush);
		} else {
			agg = new TimeBasedSingleWindowAggregate<T1, T2>(WS, WA, window);
		}
		agg.registerIn((Stream<T1>) streams.get(inKey));
		agg.registerOut((Stream<T2>) streams.get(outKey));
		operators.put(key, agg);
		return agg;
	}

	@SuppressWarnings("unchecked")
	public <T1 extends Tuple, T2 extends Tuple> Operator<T1, T2> addMapOperator(
			String identifier, MapFunction<T1, T2> mapFunction,
			StreamKey<T1> inKey, StreamKey<T2> outKey) {
		OperatorKey<T1, T2> key = new OperatorKey<T1, T2>(identifier,
				inKey.type, outKey.type);
		Operator<T1, T2> map = null;
		if (keepStatistics) {
			map = new OperatorStatistic<T1, T2>(new MapOperator<T1, T2>(
					mapFunction), statsFolder + File.separator + identifier
					+ ".proc.csv", autoFlush);
		} else {
			map = new MapOperator<T1, T2>(mapFunction);
		}
		map.registerIn((Stream<T1>) streams.get(inKey));
		map.registerOut((Stream<T2>) streams.get(outKey));
		operators.put(key, map);
		return map;
	}

	@SuppressWarnings("unchecked")
	public <T extends Tuple> Operator<T, T> addFilterOperator(
			String identifier, FilterFunction<T> filterF, StreamKey<T> inKey,
			StreamKey<T> outKey) {
		OperatorKey<T, T> key = new OperatorKey<T, T>(identifier, inKey.type,
				outKey.type);
		Operator<T, T> filter = null;
		if (keepStatistics) {
			filter = new OperatorStatistic<T, T>(
					new FilterOperator<T>(filterF), statsFolder
							+ File.separator + identifier + ".proc.csv",
					autoFlush);
		} else {
			filter = new FilterOperator<T>(filterF);
		}
		filter.registerIn((Stream<T>) streams.get(inKey));
		filter.registerOut((Stream<T>) streams.get(outKey));
		operators.put(key, filter);
		return filter;
	}

	@SuppressWarnings("unchecked")
	public <T extends Tuple> SourceKey<T> addSource(String identifier,
			Source<T> source, StreamKey<T> outKey) {
		SourceKey<T> key = new SourceKey<T>(identifier, outKey.type);
		source.registerOut((Stream<T>) streams.get(outKey));
		sources.put(key, source);
		return key;
	}

	@SuppressWarnings("unchecked")
	public <T extends Tuple> SourceKey<T> addTextSource(String identifier,
			String fileName, TextSourceFunction<T> function, StreamKey<T> outKey) {
		SourceKey<T> key = new SourceKey<T>(identifier, outKey.type);
		Source<T> source = new TextSource<T>(fileName, function);
		source.registerOut((Stream<T>) streams.get(outKey));
		sources.put(key, source);
		return key;
	}

	@SuppressWarnings("unchecked")
	public <T extends Tuple> SinkKey<T> addSink(String identifier,
			Sink<T> sink, StreamKey<T> streamKey) {
		SinkKey<T> key = new SinkKey<T>(identifier, streamKey.type);
		sink.registerIn((Stream<T>) streams.get(streamKey));
		sinks.put(key, sink);
		return key;
	}

	@SuppressWarnings("unchecked")
	public <T extends Tuple> SinkKey<T> addTextSink(String identifier,
			String fileName, TextSinkFunction<T> function,
			StreamKey<T> streamKey) {
		SinkKey<T> key = new SinkKey<T>(identifier, streamKey.type);
		Sink<T> sink = new TextSink<T>(fileName, function, true);
		sink.registerIn((Stream<T>) streams.get(streamKey));
		sinks.put(key, sink);
		return key;
	}

	@SuppressWarnings("unchecked")
	public <T extends Tuple> SinkKey<T> addTextSink(String identifier,
			String fileName, TextSinkFunction<T> function, boolean autoFlush,
			StreamKey<T> streamKey) {
		SinkKey<T> key = new SinkKey<T>(identifier, streamKey.type);
		Sink<T> sink = new TextSink<T>(fileName, function, autoFlush);
		sink.registerIn((Stream<T>) streams.get(streamKey));
		sinks.put(key, sink);
		return key;
	}

	@SuppressWarnings("unchecked")
	public <T1 extends Tuple, T2 extends Tuple, T3 extends Tuple> Operator2In<T1, T2, T3> addOperator2In(
			String identifier, BaseOperator2In<T1, T2, T3> operator,
			StreamKey<T1> in1Key, StreamKey<T2> in2Key, StreamKey<T3> outKey) {
		Operator2InKey<T1, T2, T3> key = new Operator2InKey<T1, T2, T3>(
				identifier, in1Key.type, in2Key.type, outKey.type);
		Operator2In<T1, T2, T3> op = null;
		if (keepStatistics) {
			op = new Operator2InStatistic<T1, T2, T3>(operator, statsFolder
					+ File.separator + identifier + ".proc.csv", autoFlush);
		} else {
			op = operator;
		}
		op.registerIn1((Stream<T1>) streams.get(in1Key));
		op.registerIn2((Stream<T2>) streams.get(in2Key));
		op.registerOut((Stream<T3>) streams.get(outKey));
		operators2in.put(key, op);
		return op;
	}

	@SuppressWarnings("unchecked")
	public <T1 extends RichTuple, T2 extends RichTuple, T3 extends RichTuple> Operator2In<T1, T2, T3> addJoinOperator(
			String identifier, Predicate<T1, T2, T3> predicate, long WS,
			StreamKey<T1> in1Key, StreamKey<T2> in2Key, StreamKey<T3> outKey) {
		Operator2InKey<T1, T2, T3> key = new Operator2InKey<T1, T2, T3>(
				identifier, in1Key.type, in2Key.type, outKey.type);
		Operator2In<T1, T2, T3> op = null;

		if (keepStatistics) {
			op = new Operator2InStatistic<T1, T2, T3>(
					new TimeBasedJoin<T1, T2, T3>(WS, predicate), statsFolder
							+ File.pathSeparator + identifier + ".proc.csv",
					autoFlush);
		} else {
			op = new TimeBasedJoin<T1, T2, T3>(WS, predicate);
		}
		op.registerIn1((Stream<T1>) streams.get(in1Key));
		op.registerIn2((Stream<T2>) streams.get(in2Key));
		op.registerOut((Stream<T3>) streams.get(outKey));
		operators2in.put(key, op);
		return op;
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
