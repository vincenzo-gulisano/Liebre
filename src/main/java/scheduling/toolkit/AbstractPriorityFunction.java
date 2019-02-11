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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.Validate;

public abstract class AbstractPriorityFunction implements PriorityFunction {

  private final Feature[] features;
  private double[] cache;
  private boolean caching;

  public AbstractPriorityFunction(Feature... features) {
    Validate.notEmpty(features, "Priority function has not features!");
    this.features = features;
  }

  public AbstractPriorityFunction(PriorityFunction... dependentFunctions) {
    Set<Feature> features = new HashSet<>();
    for (PriorityFunction function : dependentFunctions) {
      features.addAll(Arrays.asList(function.features()));
    }
    this.features = features.toArray(new Feature[0]);
  }

  @Override
  public double apply(Task task, double[][] features) {
    if (caching) {
      if (cache[task.getIndex()] < 0) {
        double result = applyWithCaching(task, features);
        cache[task.getIndex()] = result;
        return result;
      }
      return cache[task.getIndex()];
    }
    return applyWithCaching(task, features);
  }

  @Override
  public PriorityFunction enableCaching(int nTasks) {
    this.cache = new double[nTasks];
    this.caching = true;
    return this;
  }

  @Override
  public void clearCache() {
    if (caching) {
      for (int i = 0; i < cache.length; i++) {
        cache[i] = -1;
      }
    }
  }

  protected double applyWithCaching(Task task, double[][] features) {
    throw new UnsupportedOperationException("Caching is enabled but applyWithCaching is not "
        + "implemented!");
  }

  @Override
  public final Feature[] features() {
    return features.clone();
  }

}
