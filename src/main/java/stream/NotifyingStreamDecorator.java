package stream;

import common.component.EventType;
import common.tuple.Tuple;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.Validate;

public class NotifyingStreamDecorator<T extends Tuple> extends StreamDecorator<T> {

  final Lock lock = new ReentrantLock(false);
  private T overflow;

  public NotifyingStreamDecorator(Stream<T> stream) {
    super(stream);
  }

  @Override
  public boolean offer(T tuple) {
    lock.lock();
    try {
      if (!super.offer(tuple)) {
        Validate.validState(overflow == null);
        overflow = tuple;
        return false;
      }
      return true;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public T poll() {
    lock.lock();
    try {
      T value = super.poll();
      if (overflow != null && super.offer(overflow)) {
        value = value != null ? value : overflow;
        overflow = null;
      }
      return value;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public T peek() {
    lock.lock();
    try {
      T value = super.peek();
      value = value != null ? value : overflow;
      return value;
    } finally {
      lock.unlock();
    }
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
      if (value != null) {
        // if read succeeded, writer proceed
        getSource().notify(EventType.WRITE);
      } else {
        // if queue empty, reader wait
        getDestination().wait(EventType.READ);
      }
      return value;
    } finally {
      lock.unlock();
    }
  }
}
