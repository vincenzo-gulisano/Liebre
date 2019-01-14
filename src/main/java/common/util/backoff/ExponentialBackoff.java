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

public class ExponentialBackoff implements Backoff {

  private final int min, max, retries;
  private final Random rand = new Random();
  private int currentLimit, currentRetries;

  public ExponentialBackoff(int min, int max, int retries) {
    this.min = min;
    this.max = max;
    this.retries = retries;
    this.currentLimit = min;
    this.currentRetries = retries;
  }

  public static BackoffFactory factory(int min, int max, int retries) {
    return new Factory(min, max, retries);
  }

  @Override
  public void backoff() {
    int delay = rand.nextInt(currentLimit);
    currentRetries--;
    if (currentRetries == 0) {
      currentLimit = Math.min(2*currentLimit, max);
      currentRetries = retries;
    }

    Util.sleep(delay);
  }

  @Override
  public void relax() {
    if (currentRetries < retries) {
      currentRetries++;
      if (currentRetries == retries) {
        currentLimit = Math.max(currentLimit / 2, min);
      }
    }
  }

  private static final class Factory implements BackoffFactory {

    private final int min, max, retries;

    Factory(int min, int max, int retries) {
      this.min = min;
      this.max = max;
      this.retries = retries;
    }

    @Override
    public Backoff newInstance() {
      return new ExponentialBackoff(min, max, retries);
    }
  }

}

