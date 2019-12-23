package stream;

import common.scalegate.ScaleGate;
import common.scalegate.ScaleGateAArrImpl;
import common.scalegate.TuplesFromAll;
import component.StreamConsumer;
import component.StreamProducer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/*
 * Assumption: all writers and reader threads are distinct
 * (i.e., this class is not safe when used in combination
 * with custom thread scheduling)
 */
public class SGStream<T extends Comparable<? super T>> extends AbstractStream<T> implements
    MWMRStream<T> {

  public static final String SGSTREAM_UNSUPPORTED = "Cannot invoke this function on SGStream";

  // TODO Am I using id in the right way?
  // TODO Am I using index in the right way?
  // TODO Am I using enabled in the right way?
  // TODO Am I using sources in the right way?
  // TODO Am I using destinations in the right way?

  private ScaleGate<T> sg;
  private List<StreamProducer<T>> producers;
  private List<StreamConsumer<T>> consumers;
  private final AtomicInteger producerIndexer = new AtomicInteger(0);
  private final AtomicInteger consumerIndexer = new AtomicInteger(0);
  private Map<Integer, Integer> producerMapping = new HashMap<>();
  private Map<Integer, Integer> consumerMapping = new HashMap<>();

  private TuplesFromAll barrier;

  public SGStream(
      String id,
      int index,
      int maxLevels,
      int writers,
      int readers,
      List<StreamProducer<T>> producers,
      List<StreamConsumer<T>> consumers) {
    super(id, index);
    this.sg = new ScaleGateAArrImpl(maxLevels, writers, readers);
    this.producers = producers;
    this.consumers = consumers;

    barrier = new TuplesFromAll();
    barrier.setSize(writers);
  }

  @Override
  public void registerProducer(StreamProducer<T> producer) {
    consumerMapping.put(producer.getIndex(), producerIndexer.getAndIncrement());
  }

  @Override
  public void registerConsumer(StreamConsumer<T> consumer) {
    producerMapping.put(consumer.getIndex(), consumerIndexer.getAndIncrement());
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public void doAddTuple(T tuple, int producerIndex) {
    sg.addTuple(tuple, producerIndex);
  }

  @Override
  public T doGetNextTuple(int consumerIndex) {
    while (barrier.receivedTupleFromEachInput()) {}

    return sg.getNextReadyTuple(consumerIndex);
  }

  @Override
  public List<StreamProducer<T>> producers() {
    return producers;
  }

  @Override
  public List<StreamConsumer<T>> consumers() {
    return consumers;
  }

  @Override
  public boolean offer(T tuple, int producerIndex) {
    throw new UnsupportedOperationException(SGSTREAM_UNSUPPORTED);
  }

  @Override
  public T peek(int consumerIndex) {
    throw new UnsupportedOperationException(SGSTREAM_UNSUPPORTED);
  }

  @Override
  public int remainingCapacity() {
    return Integer.MAX_VALUE;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public void resetArrivalTime() {
    throw new UnsupportedOperationException(SGSTREAM_UNSUPPORTED);
  }

  @Override
  public double averageArrivalTime() {
    throw new UnsupportedOperationException(SGSTREAM_UNSUPPORTED);
  }
}