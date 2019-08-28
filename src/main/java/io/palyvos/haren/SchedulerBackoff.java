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

package io.palyvos.haren;

import common.util.Util;
import common.util.backoff.Backoff;
import java.util.Random;
import org.apache.commons.lang3.Validate;

/**
 * {@link Backoff} implementation that sleeps for exponentially increasing times every time backoff
 * is called. <p>This is a stateful and <emph>NOT thread-safe</emph> object.
 * <p>
 * Every time {@link SchedulerBackoff#backoff(long)} is called, the calling thread sleeps for a
 * random
 * duration in the interval {@code [0, limit]} milliseconds, respecting the passed maximum delay.
 * The range of the limit is defined in the constructor (parameters {@code min, max} and it is
 * exponentially increased for every {@code retries} consecutive calls for the backoff function.
 * A similar exponential reduction of the limit happens when consecutive calls to the relax
 * function happen.
 * </p>
 */
public class SchedulerBackoff {

  private final int min, max, retries;
  private final Random rand = new Random();
  private int currentLimit, currentRetries;

  /**
   * Construct a new {@link SchedulerBackoff} object with the given configuration. The parameters
   * given in this constructor will control the behavior of the algorithm in this specific
   * instance.
   *
   * @param min The minimum backoff limit in millisec when a {@link
   *     SchedulerBackoff#backoff(long)}
   *     is
   *     called.
   * @param max The maximum backoff limit, in millisec when a {@link
   *     SchedulerBackoff#backoff(long)}
   *     is called.
   * @param retries The number of consecutive calls to {@link SchedulerBackoff#backoff(long)} or
   *     {@link SchedulerBackoff#relax()} that will actually cause a change in the backoff time.
   */
  public SchedulerBackoff(int min, int max, int retries) {
    this.min = Math.max(min, 1);
    this.max = Math.max(max, 1);
    Validate.isTrue(this.min > 0);
    Validate.isTrue(this.min <= this.max);
    this.retries = retries;
    this.currentLimit = min;
    this.currentRetries = retries;
  }

  /**
   * Backoff, i.e., put the calling thread to sleep for an exponentially increasing number of
   * time.
   *
   * @param maxDelayMillis The maximum sleep time for this call, in milliseconds
   */
  public void backoff(long maxDelayMillis) {
    int delay = rand.nextInt(currentLimit);
    currentRetries--;
    if (currentRetries == 0) {
      currentLimit = Math.min(2 * currentLimit, max);
      currentRetries = retries;
    }
    final long sleepTime = Math.min(delay, maxDelayMillis);
    if (sleepTime > 0) {
      Util.sleep(sleepTime);
    }
  }

  /**
   * Reduce the backoff limit.
   */
  public void relax() {
    if (currentRetries < retries) {
      currentRetries++;
      if (currentRetries == retries) {
        currentLimit = Math.max(currentLimit / 2, min);
      }
    }
  }

  public void reset() {
    currentLimit = min;
    currentRetries = retries;
  }


}

