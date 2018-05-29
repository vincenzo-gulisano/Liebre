package stream.smq;

import common.tuple.Tuple;
import common.util.backoff.Backoff;
import common.util.backoff.NoopBackoff;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import stream.Stream;

public final class SmartMQReaderImpl implements SmartMQReader,
    SmartMQController {

  private static final int FREE = 0;
  private static final int LOCKED = 1;

  private List<Stream<? extends Tuple>> queues = new ArrayList<>();
  private List<Backoff> backoffs = new ArrayList<>();
  private MultiSemaphore readSemaphore;
  private volatile boolean enabled;

  @Override
  public int register(Stream<? extends Tuple> stream, Backoff backoff) {
    int index = queues.size();
    queues.add(stream);
    backoffs.add(backoff);
    return index;
  }

  @Override
  public int register(Stream<? extends Tuple> stream) {
    return register(stream, NoopBackoff.INSTANCE);  }

  @Override
  public void notifyWrite(int queueIndex) {
    readSemaphore.release(queueIndex);
    backoffs.get(queueIndex).relax();
  }

  @Override
  public <T extends Tuple> T poll(int queueIndex) {
    T value = (T) queues.get(queueIndex).poll();
    if (value == null) {
      waitRead(queueIndex);
      return null;
    }
    return value;
  }

  @Override
  public void waitRead(int queueIndex) {
    try {
      readSemaphore.acquire(queueIndex);
      backoffs.get(queueIndex).backoff();
    } catch (InterruptedException e) {
      System.out.println("waitRead() interrupted");
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public void enable() {
    this.queues = Collections.unmodifiableList(queues);
    this.backoffs = Collections.unmodifiableList(backoffs);
    this.readSemaphore = new MultiSemaphore(queues.size());
    this.enabled = true;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void disable() {
    this.enabled = false;
  }

}
