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

import common.util.Util;
import java.util.Random;

public class Backoff2 implements Backoff {

  final int min, max, retries;
  int currentLimit, currentRetries;
  private final Random rand = new Random();

  public Backoff2(int min, int max, int retries) {
    this.min = min;
    this.max = max;
    this.retries = retries;
    this.currentLimit = min;
    this.currentRetries = retries;
  }

  @Override
  public void backoff() {

    int delay = rand.nextInt(currentLimit);
    currentRetries--;
    if (currentRetries == 0) {
      currentLimit = (2 * currentLimit < max) ? 2 * currentLimit : max;
      currentRetries = retries;
    }

    Util.sleep(delay);
  }

  @Override
  public void relax() {
    if (currentRetries < retries) {
      currentRetries++;
      if (currentRetries == retries)
        currentLimit = (currentLimit / 2 >= min) ? currentLimit / 2
            : min;
    }
  }

  @Override
  public Backoff newInstance() {
    return new Backoff2(min, max, retries);
  }
}

