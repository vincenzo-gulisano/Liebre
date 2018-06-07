package stream;

import common.tuple.Tuple;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.Validate;

class AdditionalOfferBoundedStreamDecorator<T extends Tuple> extends StreamDecorator<T> {

  final Lock lock = new ReentrantLock(false);
  private T overflow;

  public AdditionalOfferBoundedStreamDecorator(BoundedStream<T> stream) {
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
    }
    finally {
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
    }
    finally {
      lock.unlock();
    }
  }


  @Override
  public void addTuple(T tuple) {
    throw new UnsupportedOperationException();
  }

  @Override
  public T getNextTuple() {
    throw new UnsupportedOperationException();
  }
}
