package stream;

import common.scalegate.ScaleGate;
import common.scalegate.ScaleGateAArrImpl;
import common.scalegate.TuplesFromAll;
import common.util.backoff.Backoff;
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

  private ScaleGate<T> sg;
  private List<? extends StreamProducer<T>> producers;
  private List<? extends StreamConsumer<T>> consumers;
  private final AtomicInteger producerIndexer = new AtomicInteger(0);
  private final AtomicInteger consumerIndexer = new AtomicInteger(0);
  private Map<Integer, Integer> producerMapping = new HashMap<>();
  private Map<Integer, Integer> consumerMapping = new HashMap<>();
  private volatile boolean isFlushed = false;

  private final Backoff readBackoff;

  private TuplesFromAll barrier;

  public SGStream(
      String id,
      int index,
      int maxLevels,
      int writers,
      int readers,
      List<? extends StreamProducer<T>> producers,
      List<? extends StreamConsumer<T>> consumers,
      Backoff backoff) {
    super(id, index);
    this.sg = new ScaleGateAArrImpl(maxLevels, writers, readers);
    this.producers = producers;
    this.consumers = consumers;

    barrier = new TuplesFromAll();
    barrier.setSize(writers);

    this.readBackoff = backoff.newInstance();
  }

  @Override
  public void registerProducer(StreamProducer<T> producer) {
    producerMapping.put(producer.getIndex(), producerIndexer.getAndIncrement());
  }

  @Override
  public void registerConsumer(StreamConsumer<T> consumer) {
    consumerMapping.put(consumer.getIndex(), consumerIndexer.getAndIncrement());
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
    barrier.receivedTupleFrom(producerMapping.get(producerIndex));
    sg.addTuple(tuple, producerMapping.get(producerIndex));
  }

  @Override
  public T doGetNextTuple(int consumerIndex) {
    while (!barrier.receivedTupleFromEachInput()) {}

    T tuple = sg.getNextReadyTuple(consumerMapping.get(consumerIndex));
    if (tuple != null) {
      readBackoff.relax();
      return tuple;
    }
    readBackoff.backoff();
    return null;
  }

  @Override
  public List<? extends StreamProducer<T>> producers() {
    return producers;
  }

  @Override
  public List<? extends StreamConsumer<T>> consumers() {
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

  @Override
  public void flush() {
    this.sg.letItFlush();
    isFlushed = true;
  }

  @Override
  public boolean isFlushed() {
    return isFlushed && this.sg.hasBeenEmptied();
  }

  @Override
  public void clear() {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented method 'clear'");
  }
}
