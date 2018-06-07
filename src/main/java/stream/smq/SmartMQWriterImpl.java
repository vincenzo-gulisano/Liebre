package stream.smq;

import common.tuple.Tuple;
import common.util.backoff.Backoff;
import common.util.backoff.NoopBackoff;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.apache.commons.lang3.Validate;
import stream.Stream;
import stream.smq.resource.ResourceManager;
import stream.smq.resource.ResourceManagerFactory;

public final class SmartMQWriterImpl implements SmartMQWriter, SmartMQController {

  private static final int FREE = 0;
  private static final int LOCKED = 1;

  private List<Stream<? extends Tuple>> queues = new ArrayList<>();
  private List<Queue<? extends Tuple>> buffers = new ArrayList<>();
  private List<Backoff> backoffs = new ArrayList<>();
  private AtomicIntegerArray bufferLocks;
  private volatile boolean enabled;
  private ResourceManager writeSemaphore;
  private final ResourceManagerFactory rmFactory;

  public SmartMQWriterImpl(ResourceManagerFactory rmFactory) {
    this.rmFactory = rmFactory;
  }

  @Override
  public int register(Stream<? extends Tuple> stream, Backoff backoff) {
    int index = queues.size();
    queues.add(stream);
    backoffs.add(backoff);
    buffers.add(new ConcurrentLinkedQueue<>());
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
    this.buffers = Collections.unmodifiableList(buffers);
    this.backoffs = Collections.unmodifiableList(backoffs);
    this.bufferLocks = new AtomicIntegerArray(queues.size());
    this.writeSemaphore =  rmFactory.newResourceManager(queues.size());
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
    Queue<T> buffer = (Queue<T>) buffers.get(queueIndex);
    buffer.add(value);
    if (!fullCopyInputBuffer(queueIndex)) {
      waitWrite(queueIndex);
    }
  }

  @Override
  public void notifyRead(int queueIndex) {
    //FIXME: Revisit this, is it correct?
    if (fullCopyInputBuffer(queueIndex)) {
      writeSemaphore.release(queueIndex);
    }
    backoffs.get(queueIndex).relax();
  }

  @Override
  public void waitWrite(int queueIndex) throws InterruptedException {
    writeSemaphore.acquire(queueIndex);
    backoffs.get(queueIndex).backoff();
  }

  private boolean fullCopyInputBuffer(int queueIndex) {
    Queue<? extends Tuple> buffer = buffers.get(queueIndex);
    if (buffer.isEmpty() || !bufferLocks.compareAndSet(queueIndex, FREE, LOCKED)) {
      // If buffer already locked, do nothing because somebody
      // is already copying it anyways
      return true;
    }
    // --- ENTER CRITICAL SECTION ---
    try {
      Stream<Tuple> destination = (Stream<Tuple>) queues.get(queueIndex);
      while (!buffer.isEmpty()) {
        Tuple value = buffer.peek();
        if (!destination.offer(value)) {
          return false;
        }
        buffer.remove();
      }
      return true;
    } finally {
      // --- LEAVE CRITICAL SECTION ---
      bufferLocks.set(queueIndex, FREE);
    }
  }


}
