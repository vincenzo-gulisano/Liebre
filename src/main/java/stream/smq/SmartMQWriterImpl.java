package stream.smq;

import common.tuple.Tuple;
import common.util.backoff.Backoff;
import common.util.backoff.NoopBackoff;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.Validate;
import stream.Stream;
import stream.smq.resource.ResourceManager;
import stream.smq.resource.ResourceManagerFactory;

public final class SmartMQWriterImpl implements SmartMQWriter, SmartMQController {

  private static final int FREE = 0;
  private static final int LOCKED = 1;
  private final ResourceManagerFactory rmFactory;
  private List<Stream<? extends Tuple>> queues = new ArrayList<>();
  private List<Backoff> backoffs = new ArrayList<>();
  private volatile boolean enabled;
  private ResourceManager writeSemaphore;

  public SmartMQWriterImpl(ResourceManagerFactory rmFactory) {
    this.rmFactory = rmFactory;
  }

  @Override
  public int register(Stream<? extends Tuple> stream, Backoff backoff) {
    int index = queues.size();
    queues.add(stream);
    backoffs.add(backoff);
    return index;
  }

  @Override
  public int register(Stream<? extends Tuple> stream) {
    return register(stream, NoopBackoff.INSTANCE);
  }

  @Override
  public void enable() {
    Validate.validState(queues.size() > 0, "queues");
    this.queues = Collections.unmodifiableList(queues);
    this.backoffs = Collections.unmodifiableList(backoffs);
    this.writeSemaphore = rmFactory.newResourceManager(queues.size());
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

  @Override
  public <T extends Tuple> void put(int queueIndex, T value) throws InterruptedException {
    if (value == null) {
      throw new IllegalArgumentException("value");
    }
    Stream<T> queue = (Stream<T>) queues.get(queueIndex);
    if (!queue.offer(value)) {
      waitWrite(queueIndex);
    }
  }

  @Override
  public void notifyRead(int queueIndex) {
    writeSemaphore.release(queueIndex);
    backoffs.get(queueIndex).relax();
  }

  @Override
  public void waitWrite(int queueIndex) throws InterruptedException {
    writeSemaphore.acquire(queueIndex);
    backoffs.get(queueIndex).backoff();
  }


}
