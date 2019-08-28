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

import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;

public class HighestRateFunction extends CombinedPriorityFunction {

  public static final int ARRIVAL_TIME_INDEX = 0;
  private long costUpdatePeriodMillis = 500;
  private long[] lastCostUpdate;

  public HighestRateFunction() {
    super(PriorityFunctions.sourceAverageArrivalTime(), PriorityFunctions.globalRate());
  }

  public void setCostUpdatePeriod(long period, TimeUnit timeUnit) {
    this.costUpdatePeriodMillis = timeUnit.toMillis(period);
  }

  @Override
  public void apply(Task task, double[][] features, double[] output) {
    Validate.isTrue(cachingEnabled(), "This function cannot work without caching!");
    long ts = System.currentTimeMillis();
    boolean updateCost = (ts - lastCostUpdate[task.getIndex()]) > costUpdatePeriodMillis;
    if (updateCost) {
      super.apply(task, features, output);
      lastCostUpdate[task.getIndex()] = ts;
      return;
    }
    // Otherwise update only arrival time
    output[ARRIVAL_TIME_INDEX] = functions[ARRIVAL_TIME_INDEX].apply(task, features);
  }

  @Override
  public MultiPriorityFunction enableCaching(int nTasks) {
    this.lastCostUpdate = new long[nTasks];
    return super.enableCaching(nTasks);
  }

  @Override
  public String name() {
    return "HIGHEST_RATE";
  }
}
