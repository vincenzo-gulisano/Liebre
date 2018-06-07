package stream.smq;

import common.tuple.Tuple;
import common.util.backoff.Backoff;
import common.util.backoff.NoopBackoff;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import stream.Stream;
import stream.smq.resource.ResourceManager;
import stream.smq.resource.ResourceManagerFactory;

public final class SmartMQReaderImpl implements SmartMQReader,
    SmartMQController {

  private static final int FREE = 0;
  private static final int LOCKED = 1;

  private List<Stream<? extends Tuple>> streams = new ArrayList<>();
  private List<Backoff> backoffs = new ArrayList<>();
  private ResourceManager readSemaphore;
  private final ResourceManagerFactory rmFactory;
  private volatile boolean enabled;


  public SmartMQReaderImpl(ResourceManagerFactory rmFactory) {
    this.rmFactory = rmFactory;
  }

  @Override
  public int register(Stream<? extends Tuple> stream, Backoff backoff) {
    int index = streams.size();
    streams.add(stream);
    backoffs.add(backoff);
    return index;
  }

  @Override
  public int register(Stream<? extends Tuple> stream) {
    return register(stream, NoopBackoff.INSTANCE);
  }


  @Override
  public <T extends Tuple> T take(int queueIndex) throws InterruptedException {
    T value = (T) streams.get(queueIndex).poll();
    if (value == null) {
      waitRead(queueIndex);
      return null;
    }
    return value;
  }

  @Override
  public void notifyWrite(int queueIndex) {
    readSemaphore.release(queueIndex);
    backoffs.get(queueIndex).relax();
  }

  @Override
  public void waitRead(int queueIndex) throws InterruptedException {
      readSemaphore.acquire(queueIndex);
      backoffs.get(queueIndex).backoff();
  }

  @Override
  public void enable() {
    this.streams = Collections.unmodifiableList(streams);
    this.backoffs = Collections.unmodifiableList(backoffs);
    this.readSemaphore = rmFactory.newResourceManager(streams.size());
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
