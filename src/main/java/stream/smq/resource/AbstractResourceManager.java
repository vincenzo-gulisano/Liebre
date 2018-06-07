package stream.smq.resource;

import java.util.concurrent.atomic.AtomicIntegerArray;

public abstract class AbstractResourceManager implements ResourceManager {
  private static final int FREE = 0;
  private static final int ACQUIRED = 1;
  private final AtomicIntegerArray acquired;


  public AbstractResourceManager(int size) {
    if (size <= 0) {
      throw new IllegalArgumentException("size");
    }
    this.acquired = new AtomicIntegerArray(size);
    initArray();
  }

  private void initArray() {
    for (int i = 0; i < acquired.length(); i++) {
      acquired.set(i, FREE);
    }
  }

  @Override
  public void acquire(int index) throws InterruptedException {
    if (acquired.compareAndSet(index, FREE, ACQUIRED)) {
      doAcquire();
    }
  }

  @Override
  public void release(int index) {
    // If acquired status for this queue set from true to false
    if (acquired.compareAndSet(index, ACQUIRED, FREE)) {
      doRelease();
    }
  }

  protected abstract void doAcquire() throws InterruptedException;
  protected abstract void doRelease();
}
