/*
 * Copyright (C) 2017-2019
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

import common.tuple.RichTuple;
import component.Component;
import component.StreamConsumer;
import component.StreamProducer;
import component.operator.Operator;
import component.operator.in1.Operator1In;
import component.operator.in1.aggregate.TimeBasedSingleWindow;
import component.operator.in1.aggregate.TimeBasedSingleWindowAggregate;
import component.operator.in1.filter.FilterFunction;
import component.operator.in1.filter.FilterOperator;
import component.operator.in1.map.FlatMapFunction;
import component.operator.in1.map.FlatMapOperator;
import component.operator.in1.map.MapFunction;
import component.operator.in1.map.MapOperator;
import component.operator.in2.Operator2In;
import component.operator.in2.join.JoinFunction;
import component.operator.in2.join.TimeBasedJoin;
import component.operator.router.BaseRouterOperator;
import component.operator.router.RouterOperator;
import component.operator.union.UnionOperator;
import component.sink.BaseSink;
import component.sink.Sink;
import component.sink.SinkFunction;
import component.sink.TextFileSinkFunction;
import component.source.BaseSource;
import component.source.Source;
import component.source.SourceFunction;
import component.source.TextFileSourceFunction;
import common.util.backoff.Backoff;
import common.util.backoff.ExponentialBackoff;

import java.util.*;

import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scheduling.LiebreScheduler;
import scheduling.basic.BasicLiebreScheduler;
import stream.BackoffStreamFactory;
import stream.MWMRStream;
import stream.Stream;
import stream.StreamFactory;

/**
 * The main execution unit. Acts as a factory for the stream {@link Component}s such as {@link
 * Operator}s, {@link Source}s and {@link Sink}s through various helper methods. It also handles the
 * connections of the components with the correct types of {@link Stream}s and the
 * activation/deactivation of the query. Activating the query also starts executing it by delegating
 * this work to the provided {@link LiebreScheduler} implementation.
 */
public final class Query {

  private static final Logger LOGGER = LogManager.getLogger();
  public static final int DEFAULT_STREAM_CAPACITY = 10000;
  public static final int DEFAULT_SGSTREAM_MAX_LEVELS = 3;
  public static final String OPERATOR = "operator";
  public static final String SOURCE = "source";
  public static final String SINK = "sink";
  private final Map<String, Operator<?, ?>> operators = new HashMap<>();
  private final Map<String, Source<?>> sources = new HashMap<>();
  private final Map<String, Sink<?>> sinks = new HashMap<>();
  private final LiebreScheduler LiebreScheduler;
  private final StreamFactory streamFactory;
  private Backoff defaultBackoff = new ExponentialBackoff(1, 10, 3);
  private boolean active;

  /** Construct. */
  public Query() {
    this(new BasicLiebreScheduler(), new BackoffStreamFactory());
  }

  /**
   * Construct.
   *
   * @param LiebreScheduler The LiebreScheduler implementation to use when executing the query after
   *     Query{@link #activate()} is called.
   */
  public Query(LiebreScheduler LiebreScheduler, StreamFactory streamFactory) {
    this.LiebreScheduler = LiebreScheduler;
    this.streamFactory = streamFactory;
  }

  /**
   * Set the parameters for the default {@link ExponentialBackoff} strategy.
   *
   * @param min The minimum backoff limit
   * @param max The maximum backoff limit
   * @param retries The number of retries before the backoff limit is updated.
   */
  public synchronized void setBackoff(int min, int max, int retries) {
    this.defaultBackoff = new ExponentialBackoff(min, max, retries);
  }

  public synchronized void setBackoff(Backoff backoff) {
    this.defaultBackoff = backoff;
  }

  public synchronized <IN, OUT> Operator<IN, OUT> addOperator(Operator1In<IN, OUT> operator) {
    saveComponent(operators, operator, "component/operator");
    return operator;
  }

  public synchronized <IN extends RichTuple, OUT extends RichTuple>
      Operator<IN, OUT> addAggregateOperator(
          String identifier,
          TimeBasedSingleWindow<IN, OUT> window,
          long windowSize,
          long windowSlide) {

    return addOperator(
        new TimeBasedSingleWindowAggregate<IN, OUT>(identifier, windowSize, windowSlide, window));
  }

  public synchronized <IN extends RichTuple, OUT extends RichTuple>
  List<Operator<IN, OUT>> addAggregateOperator(
          String identifier,
          TimeBasedSingleWindow<IN, OUT> window,
          long windowSize,
          long windowSlide,
          int parallelism) {
    assert (parallelism >= 1);
    List<Operator<IN, OUT>> result = new LinkedList<>();
    if (parallelism == 1) {
      result.add(addOperator(
              new TimeBasedSingleWindowAggregate<IN, OUT>(identifier, windowSize, windowSlide, window)));
    } else {
      for (int i = 0; i < parallelism; i++) {
        result.add(addOperator(
                new TimeBasedSingleWindowAggregate<IN, OUT>(identifier + "_" + i, windowSize, windowSlide, window)));
      }
    }
    return result;
  }

  public synchronized <IN, OUT> Operator<IN, OUT> addMapOperator(
      String identifier, MapFunction<IN, OUT> mapFunction) {
    return addOperator(new MapOperator<IN, OUT>(identifier, mapFunction));
  }

  public synchronized <IN, OUT> List<Operator<IN, OUT>> addMapOperator(
          String identifier, MapFunction<IN, OUT> mapFunction, int parallelism) {
    assert (parallelism >= 1);
    List<Operator<IN, OUT>> result = new LinkedList<>();
    if (parallelism == 1) {
      result.add(addOperator(new MapOperator<IN, OUT>(identifier, mapFunction)));
    } else {
      for (int i = 0; i < parallelism; i++) {
        result.add(addOperator(new MapOperator<IN, OUT>(identifier + "_" + i, mapFunction)));
      }
    }
    return result;
  }

  public synchronized <IN, OUT> Operator<IN, OUT> addFlatMapOperator(
      String identifier, FlatMapFunction<IN, OUT> mapFunction) {
    return addOperator(new FlatMapOperator<IN, OUT>(identifier, mapFunction));
  }

  public synchronized <IN, OUT> List<Operator<IN, OUT>> addFlatMapOperator(
          String identifier, FlatMapFunction<IN, OUT> mapFunction,int parallelism) {
    assert (parallelism >= 1);
    List<Operator<IN, OUT>> result = new LinkedList<>();
    if (parallelism == 1) {
      result.add(addOperator(new FlatMapOperator<IN, OUT>(identifier, mapFunction)));
    } else {
      for (int i = 0; i < parallelism; i++) {
        result.add(addOperator(new FlatMapOperator<IN, OUT>(identifier+"_"+i, mapFunction)));
      }
    }
    return result;
  }

  public synchronized <T> Operator<T, T> addFilterOperator(
      String identifier, FilterFunction<T> filterF) {
    return addOperator(new FilterOperator<T>(identifier, filterF));
  }

  public synchronized <T> List<Operator<T, T>> addFilterOperator(
          String identifier, FilterFunction<T> filterF,int parallelism) {
    assert (parallelism >= 1);
    List<Operator<T, T>> result = new LinkedList<>();
    if (parallelism == 1) {
      result.add(addOperator(new FilterOperator<T>(identifier, filterF)));
    } else {
      for (int i = 0; i < parallelism; i++) {
        result.add(addOperator(new FilterOperator<T>(identifier+"_"+i, filterF)));
      }
    }
    return result;
  }

  public synchronized <T> RouterOperator<T> addRouterOperator(String identifier) {
    RouterOperator<T> router = new BaseRouterOperator<T>(identifier);
    saveComponent(operators, router, OPERATOR);
    return router;
  }

  public synchronized <T> List<RouterOperator<T>> addRouterOperator(String identifier,int parallelism) {
    assert (parallelism >= 1);
    List<RouterOperator<T>> result = new LinkedList<>();
    if (parallelism == 1) {
      RouterOperator<T> router = new BaseRouterOperator<T>(identifier);
      saveComponent(operators, router, OPERATOR);
      result.add(router);
    } else {
      for (int i = 0; i < parallelism; i++) {
        RouterOperator<T> router = new BaseRouterOperator<T>(identifier+"_"+i);
        saveComponent(operators, router, OPERATOR);
        result.add(router);
      }
    }
    return result;
  }

  public synchronized <T> UnionOperator<T> addUnionOperator(UnionOperator<T> union) {
    saveComponent(operators, union, OPERATOR);
    return union;
  }

  public synchronized <T> UnionOperator<T> addUnionOperator(String identifier) {
    UnionOperator<T> union = new UnionOperator<>(identifier);
    return addUnionOperator(union);
  }

  public synchronized <T> List<UnionOperator<T>> addUnionOperator(String identifier,int parallelism) {
    assert (parallelism >= 1);
    List<UnionOperator<T>> result = new LinkedList<>();
    if (parallelism == 1) {
      UnionOperator<T> union = new UnionOperator<>(identifier);
      result.add(addUnionOperator(union));
    } else {
      for (int i = 0; i < parallelism; i++) {
        UnionOperator<T> union = new UnionOperator<>(identifier+"_"+i);
        result.add(addUnionOperator(union));
      }
    }
    return result;
  }

  public synchronized <T> Source<T> addSource(Source<T> source) {
    saveComponent(sources, source, SOURCE);
    return source;
  }

  public synchronized <T> Source<T> addBaseSource(String id, SourceFunction<T> function) {
    return addSource(new BaseSource<>(id, function));
  }

  public synchronized Source<String> addTextFileSource(String id, String path) {
    return addSource(new BaseSource<>(id, new TextFileSourceFunction(path)));
  }

  public synchronized <T> Sink<T> addSink(Sink<T> sink) {
    saveComponent(sinks, sink, SINK);
    return sink;
  }

  public synchronized <T> Sink<T> addBaseSink(String id, SinkFunction<T> sinkFunction) {
    return addSink(new BaseSink<>(id, sinkFunction));
  }

  public synchronized <T> Sink<T> addTextFileSink(String id, String path, boolean autoFlush) {
    return addSink(new BaseSink<>(id, new TextFileSinkFunction<>(path, autoFlush)));
  }

  public synchronized <OUT, IN, IN2> Operator2In<IN, IN2, OUT> addOperator2In(
      Operator2In<IN, IN2, OUT> operator) {
    saveComponent(operators, operator, "operator2in");
    return operator;
  }

  public synchronized <IN extends RichTuple, IN2 extends RichTuple, OUT extends RichTuple>
      Operator2In<IN, IN2, OUT> addJoinOperator(
          String identifier, JoinFunction<IN, IN2, OUT> joinFunction, long windowSize) {
    return addOperator2In(new TimeBasedJoin<>(identifier, windowSize, joinFunction));
  }

  public synchronized <T> Query connect(StreamProducer<T> producer, StreamConsumer<T> consumer) {
    return connect(producer, consumer, defaultBackoff);
  }

  public synchronized <T> Query connect(
      StreamProducer<T> producer, StreamConsumer<T> consumer, Backoff backoff) {
    Validate.isTrue(
        consumer instanceof Operator2In == false,
        "Error when connecting '%s': Please use connect2inXX() for Operator2In and subclasses!",
        consumer.getId());
    Stream<T> stream = getStream(producer, consumer, backoff);
    producer.addOutput(consumer, stream);
    consumer.addInput(producer, stream);
    return this;
  }

  public synchronized <T extends Comparable<? super T>> Query connect(
      List<StreamProducer<T>> producers, List<StreamConsumer<T>> consumers) {
    MWMRStream<T> stream =
        streamFactory.newMWMRStream(producers, consumers, DEFAULT_SGSTREAM_MAX_LEVELS);
    for (StreamProducer<T> producer : producers) {
      stream.registerProducer(producer);
      for (StreamConsumer<T> consumer : consumers) {
        producer.addOutput(consumer, stream);
      }
    }
    for (StreamConsumer<T> consumer : consumers) {
      stream.registerConsumer(consumer);
      for (StreamProducer<T> producer : producers) {
        consumer.addInput(producer, stream);
      }
    }
    return this;
  }

  public synchronized <T1 extends Comparable<? super T1>,T2> List<StreamConsumer<T1>> getConsumers(List<Operator<T1,T2>> ops) {
    List<StreamConsumer<T1>> c = new LinkedList<>();
    for (Operator<T1, T2> op : ops) {
      c.add(op);
    }
    return c;
  }

  public synchronized <T1,T2 extends Comparable<? super T2>> List<StreamProducer<T2>> getProducers(List<Operator<T1,T2>> ops) {
    List<StreamProducer<T2>> c = new LinkedList<>();
    for (Operator<T1, T2> op : ops) {
      c.add(op);
    }
    return c;
  }

  public synchronized <T> Query connect2inLeft(
      StreamProducer<T> producer, Operator2In<T, ?, ?> consumer) {
    return connect2inLeft(producer, consumer, defaultBackoff);
  }

  public synchronized <T> Query connect2inLeft(
      StreamProducer<T> producer, Operator2In<T, ?, ?> consumer, Backoff backoff) {
    Stream<T> stream = getStream(producer, consumer, backoff);
    producer.addOutput(consumer, stream);
    consumer.addInput(producer, stream);
    return this;
  }

  public synchronized <T> Query connect2inRight(
      StreamProducer<T> producer, Operator2In<?, T, ?> consumer) {
    return connect2inRight(producer, consumer, defaultBackoff);
  }

  public synchronized <T> Query connect2inRight(
      StreamProducer<T> producer, Operator2In<?, T, ?> consumer, Backoff backoff) {
    Stream<T> stream = getStream(producer, consumer.secondInputView(), backoff);
    producer.addOutput(consumer.secondInputView(), stream);
    consumer.addInput2(producer, stream);
    return this;
  }

  private synchronized <T> Stream<T> getStream(
      StreamProducer<T> producer, StreamConsumer<T> consumer, Backoff backoff) {
    Stream<T> stream =
        streamFactory.newStream(producer, consumer, DEFAULT_STREAM_CAPACITY, backoff);
    return stream;
  }

  /** Activate and start executing the query. */
  public synchronized void activate() {

    LOGGER.info("Activating query...");
    LOGGER.info(
        "Components: {} Sources, {} Operators, {} Sinks, {} Streams",
        sources.size(),
        operators.size(),
        sinks.size(),
        streams().size());
    LiebreScheduler.addTasks(sinks.values());
    LiebreScheduler.addTasks(operators.values());
    LiebreScheduler.addTasks(sources.values());
    LiebreScheduler.enable();
    LiebreScheduler.startTasks();
    active = true;
  }

  /** Deactivate and stop executing the query. */
  public synchronized void deActivate() {
    if (!active) {
      return;
    }
    LOGGER.info("Deactivating query...");
    LiebreScheduler.disable();
    LOGGER.info("Waiting for threads to terminate...");
    LiebreScheduler.stopTasks();
    LOGGER.info("DONE!");
    active = false;
  }

  /**
   * Get the number of sources in the query.
   *
   * @return The number of sources.
   */
  public int sourcesNumber() {
    return sources.size();
  }

  Collection<Source<?>> sources() {
    return sources.values();
  }

  private Set<Stream<?>> streams() {
    Set<Stream<?>> streams = new HashSet<>();
    for (Operator<?, ?> op : operators.values()) {
      streams.addAll(op.getInputs());
    }
    return streams;
  }

  private <T extends Component> void saveComponent(Map<String, T> map, T component, String type) {
    Validate.validState(
        !map.containsKey(component.getId()),
        "A component of type %s  with id '%s' has already been added!",
        type,
        component);
    Validate.notNull(component);
    if (component.getId().contains("_")) {
      LOGGER.warn(
          "It is best to avoid component IDs that contain an underscore because it will make it more difficult to analyze statistics date. Offending component: {}",
          component);
    }
    map.put(component.getId(), component);
  }
}
