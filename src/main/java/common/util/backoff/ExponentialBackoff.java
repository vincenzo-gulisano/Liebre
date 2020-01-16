/*
 * Copyright (C) 2017-2019
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

package common.util.backoff;

import common.util.Util;
import java.util.Random;

/**
 * {@link Backoff} implementation that sleeps for exponentially increasing times every time backoff
 * is called.
 *
 * <p>This is a stateful and <emph>NOT thread-safe</emph> object. You are advised to use {@link
 * ExponentialBackoff#newInstance()}} to create thread-local instances of the object with
 * the same configuration. <br>
 *
 * <p>Every time {@link ExponentialBackoff#backoff()} is called, the calling thread sleeps for a
 * random duration in the interval {@code [0, limit]} milliseconds. The range of the limit is
 * defined in the constructor (parameters {@code min, max} and it is exponentially increased for
 * every {@code retries} consecutive calls for the backoff function. A similar exponential reduction
 * of the limit happens when consecutive calls to the relax function happen.
 */
public class ExponentialBackoff implements Backoff {

  private final int min, max, retries;
  private final Random rand = new Random();
  private int currentLimit, currentRetries;

  /**
   * Construct a new {@link ExponentialBackoff} object with the given configuration. The parameters
   * given in this constructor will control the behavior of the algorithm in this specific instance.
   *
   * @param min The minimum backoff limit in millisec when a {@link ExponentialBackoff#backoff()} is
   *     called.
   * @param max The maximum backoff limit, in millisec when a {@link ExponentialBackoff#backoff()}
   *     is called.
   * @param retries The number of consecutive calls to {@link ExponentialBackoff#backoff()} or
   *     {@link ExponentialBackoff#relax()} that will actually cause a change in the backoff time.
   */
  public ExponentialBackoff(int min, int max, int retries) {
    this.min = min;
    this.max = max;
    this.retries = retries;
    this.currentLimit = min;
    this.currentRetries = retries;
  }

  /**
   * Backoff, i.e., put the calling thread to sleep for an exponentially increasing number of time.
   */
  @Override
  public void backoff() {
    int delay = rand.nextInt(currentLimit);
    currentRetries--;
    if (currentRetries == 0) {
      currentLimit = Math.min(2 * currentLimit, max);
      currentRetries = retries;
    }

    Util.sleep(delay);
  }

  /** Reduce the backoff limit. */
  @Override
  public void relax() {
    if (currentRetries < retries) {
      currentRetries++;
      if (currentRetries == retries) {
        currentLimit = Math.max(currentLimit / 2, min);
      }
    }
  }

  @Override
  public Backoff newInstance() {
    return new ExponentialBackoff(min, max, retries);
  }
}
