package stream.smq;

import common.component.EventType;
import common.tuple.Tuple;
import stream.Stream;

public class NotifyingStream<T extends Tuple> extends ExpandableStream<T> {

  public NotifyingStream(Stream<T> stream) {
    super(stream);
  }

  @Override
  public void addTuple(T tuple) {
    lock.lock();
    try {
      if (!offer(tuple)) {
        // If queue full, writer wait
        getSource().wait(EventType.WRITE);
      }
      // Notify the reader to proceed
      getDestination().notify(EventType.READ);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public T getNextTuple() {
    lock.lock();
    try {
      T value = poll();
      if (value == null) {
        // if queue empty, reader wait
        getDestination().wait(EventType.READ);
      }
      else if (!super.isFull()) {
        // if read succeeded and stream has space, writer proceed
        getSource().notify(EventType.WRITE);
      }
      return value;
    } finally {
      lock.unlock();
    }
  }

}
