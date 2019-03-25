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

package scheduling.toolkit;

import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;

/**
 * The full chain scheduling policy, e.g. chain + arrival time, with chain slowly changing.
 */
public class ChainArrivalTimeFunction extends CombinedPriorityFunction {

  public static final int ARRIVAL_TIME_INDEX = 1;
  private static SinglePriorityFunction sourcesLast = new AbstractPriorityFunction("SOURCES_LAST"
      , Feature.COMPONENT_TYPE) {
    @Override
    public double apply(Task task, double[][] features) {
      if (Feature.COMPONENT_TYPE.get(task, features) == FeatureHelper.CTYPE_SOURCE) {
        return 0;
      } else {
        return 1;
      }
    }
  };
  private long chainUpdatePeriodMillis = 250;
  private long[] lastChainUpdate;

  public ChainArrivalTimeFunction() {
    super(sourcesLast, PriorityFunctions.chain(), PriorityFunctions.headArrivalTime());
  }

  public void setCostUpdatePeriod(long period, TimeUnit timeUnit) {
    this.chainUpdatePeriodMillis = timeUnit.toMillis(period);
  }

  @Override
  public void apply(Task task, double[][] features, double[] output) {
    Validate.isTrue(cachingEnabled(), "This function cannot work without caching!");
    long ts = System.currentTimeMillis();
    boolean updateCost = (ts - lastChainUpdate[task.getIndex()]) > chainUpdatePeriodMillis;
    if (updateCost) {
      super.apply(task, features, output);
      lastChainUpdate[task.getIndex()] = ts;
      return;
    }
    // Otherwise update only arrival time
    output[ARRIVAL_TIME_INDEX] = functions[ARRIVAL_TIME_INDEX].apply(task, features);
  }

  @Override
  public MultiPriorityFunction enableCaching(int nTasks) {
    this.lastChainUpdate = new long[nTasks];
    return super.enableCaching(nTasks);
  }

  @Override
  public String name() {
    return "CHAIN_ARRIVAL_TIME";
  }

}
