/*
 * Copyright (C) 2017-2018
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package query;

import common.StreamConsumer;
import common.StreamProducer;
import common.component.Component;
import common.tuple.RichTuple;
import common.tuple.Tuple;
import common.util.backoff.Backoff2;
import common.util.backoff.BackoffFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scheduling.Scheduler;
import scheduling.impl.NoopScheduler;
import sink.BaseSink;
import sink.Sink;
import sink.SinkFunction;
import sink.SinkStatistic;
import sink.TextFileSink;
import sink.TextSinkFunction;
import source.BaseSource;
import source.Source;
import source.SourceFunction;
import source.SourceStatistic;
import source.TextFileSource;
import source.TextSourceFunction;
import stream.NotifyingStream;
import stream.Stream;
import stream.StreamFactory;
import stream.StreamStatistic;
import stream.UnboundedStream;

public final class Query {
  //FIXME: Validate Component IDs so that they do not contain _

  private static final Logger LOGGER = LogManager.getLogger();
  private static final int DEFAULT_STREAM_CAPACITY = 10000;

  private final Map<String, Operator<? extends Tuple, ? extends Tuple>> operators = new HashMap<>();
  private final Map<String, Operator2In<? extends Tuple, ? extends Tuple, ? extends Tuple>> operators2in = new HashMap<>();
  private final Map<String, Source<? extends Tuple>> sources = new HashMap<>();
  private final Map<String, Sink<? extends Tuple>> sinks = new HashMap<>();

  private final Scheduler scheduler;
  private boolean keepStatistics = false;
  private String statisticsFolder;
  private boolean autoFlush;
  private StreamFactory streamFactory;
  private BackoffFactory defaultBackoff = BackoffFactory.NOOP;

  public Query() {
    this(new NoopScheduler());
  }

  public Query(Scheduler scheduler) {
    this.scheduler = scheduler;
    activateBackoff(1, 20, 5);
    if (scheduler.usesNotifications()) {
      streamFactory = NotifyingStream.factory();
    } else {
      streamFactory = UnboundedStream.factory();
    }
  }

  public void activateStatistics(String statisticsFolder, boolean autoFlush) {
    LOGGER.info("Statistics will be saved at {}", statisticsFolder);
    this.keepStatistics = true;
    this.statisticsFolder = statisticsFolder;
    this.autoFlush = autoFlush;
  }

  public void activateSchedulingStatistics(String statisticsFolder) {
    LOGGER.info("Scheduling Statistics will be saved at {}", statisticsFolder);
    this.scheduler.activateStatistics(statisticsFolder);
  }

  public void activateStatistics(String statisticsFolder) {
    activateStatistics(statisticsFolder, true);
  }

  public void activateBackoff(int min, int max, int retries) {
    this.defaultBackoff = Backoff2.factory(min, max, retries);
  }

  public <IN extends Tuple, OUT extends Tuple> Operator<IN, OUT> addOperator(
      Operator1In<IN, OUT> operator) {
    Operator<IN, OUT> op = operator;
    if (keepStatistics) {
      op = new Operator1InStatistic<IN, OUT>(operator, statisticsFolder, autoFlush);
    }
    saveComponent(operators, operator, "operator");
    return op;
  }

  public <IN extends RichTuple, OUT extends RichTuple> Operator<IN, OUT> addAggregateOperator(
      String identifier,
      TimeBasedSingleWindow<IN, OUT> window, long windowSize, long windowSlide) {

    return addOperator(
        new TimeBasedSingleWindowAggregate<IN, OUT>(identifier, streamFactory, windowSize,
            windowSlide, window));
  }

  public <IN extends Tuple, OUT extends Tuple> Operator<IN, OUT> addMapOperator(String identifier,
      MapFunction<IN, OUT> mapFunction) {
    return addOperator(new MapOperator<IN, OUT>(identifier, streamFactory, mapFunction));
  }

  public <IN extends Tuple, OUT extends Tuple> Operator<IN, OUT> addFlatMapOperator(String identifier,
      FlatMapFunction<IN, OUT> mapFunction) {
    return addOperator(new FlatMapOperator<IN, OUT>(identifier, streamFactory, mapFunction));
  }

  public <T extends Tuple> Operator<T, T> addFilterOperator(String identifier,
      FilterFunction<T> filterF) {
    return addOperator(new FilterOperator<T>(identifier, streamFactory, filterF));
  }

  public <T extends Tuple> Operator<T, T> addRouterOperator(String identifier,
      RouterFunction<T> routerF) {
    RouterOperator<T> router = new BaseRouterOperator<T>(identifier, streamFactory, routerF);
    // Notice that the router is a special case which needs a dedicated
    // statistics operator
    if (keepStatistics) {
      router = new RouterOperatorStatistic<T>(router, statisticsFolder, autoFlush);
    }
    saveComponent(operators, router, "operator");
    return router;
  }

  public <T extends Tuple> UnionOperator<T> addUnionOperator(UnionOperator<T> union) {
    saveComponent(operators, union, "operator");
    return union;
  }

  public <T extends Tuple> UnionOperator<T> addUnionOperator(String identifier) {
    UnionOperator<T> union = new UnionOperator<>(identifier, streamFactory);
    return addUnionOperator(union);
  }

  public <T extends Tuple> Source<T> addSource(Source<T> source) {
    Source<T> s = source;
    if (keepStatistics) {
      s = new SourceStatistic<T>(s, streamFactory, statisticsFolder);
    }
    saveComponent(sources, s, "source");
    return s;
  }

  public <T extends Tuple> Source<T> addBaseSource(String id, SourceFunction<T> function) {
    return addSource(new BaseSource<>(id, function));
  }

  public <T extends Tuple> Source<T> addTextFileSource(String id, String filename,
      TextSourceFunction<T> function) {
    return addSource(new TextFileSource(id, filename, function));
  }

  public <T extends Tuple> Sink<T> addSink(Sink<T> sink) {
    Sink<T> s = sink;
    if (keepStatistics) {
      s = new SinkStatistic<T>(sink, statisticsFolder);
    }
    saveComponent(sinks, s, "sink");
    return s;
  }

  public <T extends Tuple> Sink<T> addBaseSink(String id, SinkFunction<T> sinkFunction) {
    return addSink(new BaseSink<>(id, sinkFunction));
  }

  public <T extends Tuple> Sink<T> addTextFileSink(String id, String file,
      TextSinkFunction<T> function) {
    return addSink(new TextFileSink<>(id, file, function));
  }

  public <OUT extends Tuple, IN extends Tuple, IN2 extends Tuple> Operator2In<IN, IN2, OUT> addOperator2In(
      Operator2In<IN, IN2, OUT> operator) {
    Operator2In<IN, IN2, OUT> op = operator;
    if (keepStatistics) {
      op = new Operator2InStatistic<IN, IN2, OUT>(operator, statisticsFolder, autoFlush);
    }
    saveComponent(operators2in, op, "operator2in");
    return op;
  }

  public <IN extends RichTuple, IN2 extends RichTuple, OUT extends RichTuple> Operator2In<IN, IN2, OUT> addJoinOperator(
      String identifier, Predicate<IN, IN2, OUT> predicate, long windowSize) {
    return addOperator2In(
        new TimeBasedJoin<IN, IN2, OUT>(identifier, streamFactory, windowSize, predicate));
  }

  public <T extends Tuple> Query connect(StreamProducer<T> source, StreamConsumer<T> destination) {
    return connect(source, destination, defaultBackoff);
  }

  public <T extends Tuple> Query connect(StreamProducer<T> source, StreamConsumer<T> destination,
      BackoffFactory backoff) {
    Validate.isTrue(destination instanceof Operator2In == false,
        "Error when connecting '%s': Please use connect2inXX() for Operator2In and subclasses!",
        destination.getId());
    Stream<T> stream = getStream(source, destination, backoff);
    source.addOutput(destination, stream);
    destination.addInput(source, stream);
    return this;
  }

  public <T extends Tuple> Query connect2inLeft(StreamProducer<T> source,
      Operator2In<T, ?, ?> destination) {
    return connect2inLeft(source, destination, defaultBackoff);
  }

  public <T extends Tuple> Query connect2inLeft(StreamProducer<T> source,
      Operator2In<T, ?, ?> destination, BackoffFactory backoff) {
    Stream<T> stream = getStream(source, destination, backoff);
    source.addOutput(destination, stream);
    destination.addInput(source, stream);
    return this;
  }

  public <T extends Tuple> Query connect2inRight(StreamProducer<T> source,
      Operator2In<?, T, ?> destination) {
    return connect2inRight(source, destination, defaultBackoff);
  }

  public <T extends Tuple> Query connect2inRight(StreamProducer<T> source,
      Operator2In<?, T, ?> destination, BackoffFactory backoff) {
    Stream<T> stream = getStream(source, destination.secondInputView(), backoff);
    source.addOutput(destination.secondInputView(), stream);
    destination.addInput2(source, stream);
    return this;
  }

  private <T extends Tuple> Stream<T> getStream(StreamProducer<T> source,
      StreamConsumer<T> destination,
      BackoffFactory backoff) {
    Stream<T> stream = streamFactory.newStream(source, destination, DEFAULT_STREAM_CAPACITY, backoff);
    if (keepStatistics) {
      return new StreamStatistic<>(stream, statisticsFolder, autoFlush);
    }
    else {
      return stream;
    }
  }

  public void activate() {

    LOGGER.info("Activating query...");
    LOGGER.info("Components: {} Sources, {} Operators, {} Sinks, {} Streams", sources.size(),
        operators.size() + operators2in.size(), sinks.size(), getAllStreams().size());
    scheduler.addTasks(sinks.values());
    scheduler.addTasks(allOperators());
    scheduler.addTasks(sources.values());
    scheduler.enable();
    scheduler.startTasks();
  }

  public void deActivate() {
    LOGGER.info("Deactivating query...");
    scheduler.disable();
    LOGGER.info("Waiting for threads to terminate...");
    scheduler.stopTasks();
    LOGGER.info("DONE!");
  }

  private Set<Stream<?>> getAllStreams() {
    Set<Stream<?>> streams = new HashSet<>();
    for (Operator<?, ?> op : allOperators()) {
      streams.addAll(op.getInputs());
    }
    return streams;
  }

  private Collection<Operator<?, ?>> allOperators() {
    List<Operator<?, ?>> allOperators = new ArrayList<>(operators.values());
    allOperators.addAll(this.operators2in.values());
    return allOperators;
  }

  private <T extends Component> void saveComponent(Map<String, T> map, T component, String type) {
    Validate.validState(!map.containsKey(component.getId()),
        "A component of type %s  with id '%s' has already been added!", type, component);
    map.put(component.getId(), component);
  }

}
