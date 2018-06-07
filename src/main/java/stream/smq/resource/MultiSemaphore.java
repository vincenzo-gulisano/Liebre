package stream.smq.resource;

import java.util.concurrent.Semaphore;

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
public class MultiSemaphore extends AbstractResourceManager {

  private final Semaphore semaphore;

  /**
   * Construct a new instance with the given size and permits. Note that, in constrast with the
   * {@link Semaphore} definition, the permits given are equal to {@code size - 1}
   *
   * @param size The number of unique objects that this semaphore controls access to.
   */
  public MultiSemaphore(int size) {
    super(size);
    semaphore = new Semaphore(size - 1);
  }

  @Override
  protected void doAcquire() throws InterruptedException {
    semaphore.acquire();
  }

  @Override
  protected void doRelease() {
    semaphore.release();
  }


}
