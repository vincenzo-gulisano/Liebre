/*
 * Copyright (C) 2017-2018
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

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
