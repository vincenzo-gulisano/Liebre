package experimental.provenance;

import component.Component;
import component.StreamConsumer;
import component.StreamProducer;
import component.operator.Operator;
import component.operator.in1.aggregate.TimeAggregate;
import component.operator.in1.aggregate.TimeMWAggregate;
import component.operator.in1.aggregate.TimeSWAggregate;
import component.operator.in1.aggregate.TimeWindowAdd;
import component.operator.in1.aggregate.TimeWindowAddRemove;
import component.operator.in1.aggregate.TimeWindowAddSlide;
import component.operator.in1.aggregate.TupleAggregate;
import component.operator.in1.aggregate.Window;
import component.operator.in1.filter.FilterOperator;
import component.operator.in1.map.FlatMapOperator;
import component.operator.in1.map.MapOperator;
import component.operator.in2.Operator2In;
import component.operator.in2.join.TimeBasedJoin;
import component.operator.router.BaseRouterOperator;
import component.operator.router.HashBasedRouterOperator;
import component.operator.union.UnionOperator;
import component.sink.BaseSink;
import component.sink.Sink;
import component.source.BaseSource;
import component.source.Source;
import java.util.IdentityHashMap;
import java.util.Map;
import query.Query;
import query.QueryConnection;

public class ProvenanceQueryTransformer {

  public Query transform(Query original) {
    if (original.isActive()) {
      throw new IllegalStateException("Cannot transform an active query");
    }

    Query transformed = new Query(original.streamCapacity);
    Map<Component, Component> components = new IdentityHashMap<>();

    for (Source<?> source : original.sources()) {
      components.put(source, transformSource(source, transformed));
    }
    for (Operator<?, ?> operator : original.operators()) {
      components.put(operator, transformOperator(operator, transformed));
    }
    for (Sink<?> sink : original.sinks()) {
      components.put(sink, transformSink(sink, transformed));
    }
    for (QueryConnection connection : original.connections()) {
      transformConnection(connection, components, transformed);
    }

    return transformed;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private Source<?> transformSource(Source<?> source, Query transformed) {
    if (!(source instanceof BaseSource)) {
      throw unsupported(source);
    }
    Source<?> transformedSource =
        transformed.addBaseSource(
            source.getId(), new GenealogSourceFunction(((BaseSource) source).getFunction()));
    transformedSource.setPriority(source.getPriority());
    return transformedSource;
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private Sink<?> transformSink(Sink<?> sink, Query transformed) {
    if (!(sink instanceof BaseSink)) {
      throw unsupported(sink);
    }
    return transformed.addBaseSink(sink.getId(), ((BaseSink) sink).getFunction());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private Operator<?, ?> transformOperator(Operator<?, ?> operator, Query transformed) {
    if (operator instanceof MapOperator) {
      MapOperator map = (MapOperator) operator;
      return transformed.addOperator(
          new MapOperator(operator.getId(), new GenealogMapFunction(map.getMapFunction())));
    }
    if (operator instanceof FlatMapOperator) {
      FlatMapOperator flatMap = (FlatMapOperator) operator;
      return transformed.addOperator(
          new FlatMapOperator(
              operator.getId(), new GenealogFlatMapFunction(flatMap.getFlatMapFunction())));
    }
    if (operator instanceof FilterOperator) {
      FilterOperator filter = (FilterOperator) operator;
      return transformed.addOperator(new FilterOperator(operator.getId(), filter.getFilterFunction()));
    }
    if (operator instanceof TimeBasedJoin) {
      TimeBasedJoin join = (TimeBasedJoin) operator;
      return transformed.addOperator2In(
          new TimeBasedJoin(
              operator.getId(), join.getWindowSize(), new GenealogJoinFunction(join.getJoinFunction())));
    }
    if (operator instanceof TimeMWAggregate) {
      TimeMWAggregate aggregate = (TimeMWAggregate) operator;
      TimeWindowAdd wrappedWindow = wrapTimeWindowAdd((TimeWindowAdd) aggregate.getWindow());
      TimeMWAggregate transformedAggregate =
          new TimeMWAggregate(
              operator.getId(),
              aggregate.getInstance(),
              aggregate.getParallelismDegree(),
              aggregate.getWindowSize(),
              aggregate.getWindowSlide(),
              wrappedWindow);
      transformedAggregate.registerKeyExtractor(aggregate.getKeyExtractor());
      return transformed.addOperator(transformedAggregate);
    }
    if (operator instanceof TimeSWAggregate) {
      TimeSWAggregate aggregate = (TimeSWAggregate) operator;
      Window window = aggregate.getWindow();
      TimeSWAggregate transformedAggregate;
      if (window instanceof TimeWindowAddRemove) {
        transformedAggregate =
            new TimeSWAggregate(
                operator.getId(),
                aggregate.getInstance(),
                aggregate.getParallelismDegree(),
                aggregate.getWindowSize(),
                aggregate.getWindowSlide(),
                wrapTimeWindowAddRemove((TimeWindowAddRemove) window));
      } else if (window instanceof TimeWindowAddSlide) {
        transformedAggregate =
            new TimeSWAggregate(
                operator.getId(),
                aggregate.getInstance(),
                aggregate.getParallelismDegree(),
                aggregate.getWindowSize(),
                aggregate.getWindowSlide(),
                wrapTimeWindowAddSlide((TimeWindowAddSlide) window));
      } else {
        throw unsupported(operator);
      }
      transformedAggregate.registerKeyExtractor(aggregate.getKeyExtractor());
      return transformed.addOperator(transformedAggregate);
    }
    if (operator instanceof TupleAggregate) {
      throw unsupported(operator);
    }
    if (operator instanceof HashBasedRouterOperator) {
      return transformed.addOperator(new HashBasedRouterOperator(operator.getId()));
    }
    if (operator instanceof BaseRouterOperator) {
      return transformed.addOperator(new BaseRouterOperator(operator.getId()));
    }
    if (operator instanceof UnionOperator) {
      return transformed.addUnionOperator(new UnionOperator(operator.getId()));
    }
    throw unsupported(operator);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private void transformConnection(
      QueryConnection connection, Map<Component, Component> components, Query transformed) {
    if (!connection.isSingleProducerSingleConsumer()) {
      throw new UnsupportedOperationException(
          String.format("Unsupported multi-producer/multi-consumer stream: %s", connection.getStreamId()));
    }

    Component producer = components.get(connection.getProducer());
    Component consumer = components.get(connection.getConsumer());
    if (!(producer instanceof StreamProducer) || !(consumer instanceof StreamConsumer)) {
      throw new IllegalStateException(
          String.format(
              "Could not resolve transformed endpoints for stream: %s", connection.getStreamId()));
    }

    switch (connection.getInputPort()) {
      case LEFT:
        transformed.connect2inLeft(
            (StreamProducer) producer, (Operator2In) consumer, connection.getBackoff());
        break;
      case RIGHT:
        transformed.connect2inRight(
            (StreamProducer) producer, (Operator2In) consumer, connection.getBackoff());
        break;
      case DEFAULT:
        transformed.connect(
            (StreamProducer) producer, (StreamConsumer) consumer, connection.getBackoff());
        break;
      default:
        throw new IllegalStateException("Unknown input port: " + connection.getInputPort());
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private TimeWindowAdd wrapTimeWindowAdd(TimeWindowAdd window) {
    return new GenealogTimeWindowAdd(window);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private TimeWindowAddRemove wrapTimeWindowAddRemove(TimeWindowAddRemove window) {
    return new GenealogAggregateWindow(window);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private TimeWindowAddSlide wrapTimeWindowAddSlide(TimeWindowAddSlide window) {
    return new GenealogTimeWindowAddSlide(window);
  }

  private UnsupportedOperationException unsupported(Component component) {
    return new UnsupportedOperationException(
        String.format(
            "Unsupported component for provenance transformation: %s (%s)",
            component.getId(), component.getClass().getName()));
  }
}
