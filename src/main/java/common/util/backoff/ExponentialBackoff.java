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

package common.util.backoff;

import static common.util.Util.sleep;

import java.util.Random;

public class ExponentialBackoff implements Backoff {

  private static final int INTEGER_RANGE_MAX_SHIFT = 30;
  private final int maxShift;
  private final long initialSleepMs;
  private final Random random = new Random();
  private int shift;
  private static final int MAX_RETRIES = 3;
  private int retries;

  public ExponentialBackoff(long initialSleepMs, int maxShift) {
    if (maxShift > INTEGER_RANGE_MAX_SHIFT) {
      throw new IllegalArgumentException(
          String.format("maxShift cannot be greater than %d", INTEGER_RANGE_MAX_SHIFT));
    }
    this.initialSleepMs = initialSleepMs;
    this.maxShift = maxShift;
  }

  @Override
  public void backoff() {
    shift = Math.min(shift + 1, maxShift);
    int multiplier = 1 + random.nextInt(1 << shift);
    sleep(initialSleepMs * multiplier);
  }

  @Override
  public void relax() {
    if (retries++ > MAX_RETRIES) {
      retries = 0;
      shift = Math.max(shift - 1, 0);
    }
  }


}
