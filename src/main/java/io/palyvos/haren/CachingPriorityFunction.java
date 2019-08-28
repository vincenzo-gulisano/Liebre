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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class CachingPriorityFunction extends AbstractPriorityFunction {

  private double[] cache;
  private boolean caching;
  private static final Logger LOG = LogManager.getLogger();

  public CachingPriorityFunction(String name, Feature... features) {
    super(name, features);
  }

  public CachingPriorityFunction(String name,
      SinglePriorityFunction... dependentFunctions) {
    super(name, dependentFunctions);
  }

  @Override
  public final double apply(Task task, double[][] features) {
    if (caching) {
      if (cache[task.getIndex()] < 0) {
        double result = applyWithCachingSupport(task, features);
        cache[task.getIndex()] = result;
        return result;
      }
      return cache[task.getIndex()];
    }
    return applyWithCachingSupport(task, features);
  }

  protected abstract double applyWithCachingSupport(Task task, double[][] features);

  @Override
  public SinglePriorityFunction enableCaching(int nTasks) {
    LOG.info("Caching enabled for {}", name());
    super.enableCaching(nTasks);
    this.cache = new double[nTasks];
    this.caching = true;
    return this;
  }

  @Override
  public void clearCache() {
    super.clearCache();
    if (caching) {
      for (int i = 0; i < cache.length; i++) {
        cache[i] = -1;
      }
    }
  }

  @Override
  public boolean cachingEnabled() {
    return caching;
  }
}
