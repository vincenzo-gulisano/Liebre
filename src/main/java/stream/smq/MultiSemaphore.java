package stream.smq;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * A {@link Semaphore}-like object that controls access to <b>named</b> resources that based not
 * only on their number but also on their ID. It only blocks when all resources with different IDs
 * have been acquired. Multiple consequent acquisitions or releases of the same resource have no
 * effect. <br /> Example:
 * <pre>
 *  MultiSemaphore s = new MultiSemaphore(2);
 *  s.acquire(0);
 *  // ...
 *  s.acquire(0); // Has no effect, already acquired
 *  s.acquire(1); // Blocks
 *  // ..
 *  s.release(0); // unblocks
 *  s.releaase(1);
 *  // ..
 *  s.release(1); // has no effect
 * </pre>
 *
 * @author palivosd
 */
public class MultiSemaphore {

  private static final int FREE = 0;
  private static final int ACQUIRED = 1;
  private final Semaphore semaphore;
  private final AtomicIntegerArray acquired;

  /**
   * Construct a new instance with the given size and permits. Note that, in constrast with the
   * {@link Semaphore} definition, the permits given are equal to {@code size - 1}
   *
   * @param size The number of unique objects that this semaphore controls access to.
   */
  public MultiSemaphore(int size) {
    if (size <= 0) {
      throw new IllegalArgumentException("size");
    }
    this.semaphore = new Semaphore(size - 1);
    this.acquired = new AtomicIntegerArray(size);
    initArray();
  }

  private void initArray() {
    for (int i = 0; i < acquired.length(); i++) {
      acquired.set(i, FREE);
    }
  }

  /**
   * Acquire the resource with the given index. If this is the last available resource of the
   * semaphore, the current thread will block.
   *
   * @param index The index of the resource
   * @throws InterruptedException when the acquired thread is interrupted
   */
  public void acquire(int index) throws InterruptedException {
    if (acquired.compareAndSet(index, FREE, ACQUIRED)) {
      // Down the semaphore
      semaphore.acquire();
    }
  }

  /**
   * Release the resource with the given index. If the thread was acquired due to this semaphore, it
   * will be woken up.
   *
   * @param index The index of the resource to release.
   */
  public void release(int index) {
    // If acquired status for this queue set from true to false
    if (acquired.compareAndSet(index, ACQUIRED, FREE)) {
      // Up the semaphore
      semaphore.release();
    }
  }
}
