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
package common.util;

import org.apache.commons.lang3.Validate;

/**
 * Helper class that forces a process to limits its (approximate) rate to a specific number of calls
 * per second. The thread that needs to be limited needs to call the {@link #limit(long)} function
 * at every call and the {@link RateLimiter} will force it to sleep for the appropriate amount of
 * time when it starts exceeding the desired rate.
 *
 * <br />
 *
 * To prevent a spike in the calls at the beginnng of each second and a long sleep afterwards, the
 * actual implementation counts the calls in custom time intervals which can be as small or as big
 * as deemed appropriate for the application.
 *
 * @author palivosd
 */
public final class RateLimiter {

  private final long intervalMillis;
  private volatile long lastInterval;
  private volatile long callsNumber;

  /**
   * Construct with a default interval of {@code 100} ms.
   */
  public RateLimiter() {
    this(100);
  }

  /**
   * Construct with the given custom interval
   *
   * @param intervalMillis The interval where the limit measurements take place, i.e. the time
   * "resolution" of the limiter.
   */
  public RateLimiter(long intervalMillis) {
    Validate.inclusiveBetween(1, 1000, intervalMillis);
    this.intervalMillis = intervalMillis;
    this.lastInterval = System.currentTimeMillis() / intervalMillis;
  }

  /**
   * Limit the rate of the calling thread. If more {@code rate} calls/(interval millisec) have been
   * made to this function during the limiter time interval, the calling thread will sleep until the
   * next interval.
   *
   * @param rate The maximum rate of the calling function, <b>per second</b>
   */
  public void limit(long rate) {
    if (updateInterval()) {
      return;
    }
    if (++callsNumber >= getConvertedRate(rate)) {
      waitUntilNextInterval();
    }
  }

  /**
   * Update the counters depending on whether the current time belongs in the current or in the next
   * interval
   */
  private final boolean updateInterval() {
    long currentInterval = System.currentTimeMillis() / intervalMillis;
    if (lastInterval == currentInterval) {
      return false;
    } else {
      lastInterval = currentInterval;
      callsNumber = 0;
      return true;
    }
  }

  /**
   * Convert tuples/second to tuples/interval
   *
   * @param rate The rate in tuples/second
   * @return The rate in tuples/interval
   */
  private final long getConvertedRate(long rate) {
    return (intervalMillis * rate) / 1000;
  }

  /**
   * Wait until the next interval. When this function returns, the interval will have been updated
   * to a new value and the calls will be reset to 0.
   */
  private void waitUntilNextInterval() {
    while (!updateInterval()) {
      long millisToSleep = millisUntilNextInterval();
      if (millisToSleep > 0) {
        Util.sleep(millisToSleep);
      }
    }
  }

  private long millisUntilNextInterval() {
    return (lastInterval + 1) * intervalMillis - System.currentTimeMillis();
  }
}
