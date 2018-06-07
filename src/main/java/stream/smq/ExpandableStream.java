package stream.smq;

import common.tuple.Tuple;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import stream.Stream;
import stream.StreamDecorator;

public class ExpandableStream<T extends Tuple> extends StreamDecorator<T> {

  final Lock lock = new ReentrantLock(false);
  private final Queue<T> buffer = new ConcurrentLinkedQueue<>();

  public ExpandableStream(Stream<T> stream) {
    super(stream);
  }

  @Override
  public boolean offer(T tuple) {
    lock.lock();
    try {
      fullCopyBuffer();
      if (!super.offer(tuple)) {
        buffer.offer(tuple);
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
      fullCopyBuffer();
      T value = super.poll();
      return value;
    } finally {
      lock.unlock();
    }
  }

  @Override
  public T peek() {
    lock.lock();
    try {
      T queuePeek = super.peek();
      T value = queuePeek != null ? queuePeek : buffer.peek();
      return value;
    } finally {
      lock.unlock();
    }
  }

  private final void fullCopyBuffer() {
    lock.lock();
    try {
      while (!buffer.isEmpty()) {
        T value = buffer.peek();
        if (!super.offer(value)) {
          return;
        }
        buffer.remove();
      }
    } finally {
      lock.unlock();
    }
  }

  @Override
  public int size() {
    lock.lock();
    try {
      return super.size() + buffer.size();
    } finally {
      lock.unlock();
    }
  }
}
