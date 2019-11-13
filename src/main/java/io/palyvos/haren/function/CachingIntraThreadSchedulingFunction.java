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

package io.palyvos.haren.function;

import io.palyvos.haren.Features;
import io.palyvos.haren.Task;
import io.palyvos.haren.TaskIndexer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Base implementation of an {@link AbstractIntraThreadSchedulingFunction} with caching support.
 */
public abstract class CachingIntraThreadSchedulingFunction extends
    AbstractIntraThreadSchedulingFunction {

  private static final Logger LOG = LogManager.getLogger();
  private double[] cache;
  private boolean caching;

  /**
   * Construct.
   *
   * @param name The function's name.
   * @param features The features used by this function.
   */
  public CachingIntraThreadSchedulingFunction(String name, Features... features) {
    super(name, features);
  }

  /**
   * Construct.
   *
   * @param name The functions name.
   * @param dependentFunctions Other {@link SingleIntraThreadSchedulingFunction}s used by this
   *     function.
   */
  public CachingIntraThreadSchedulingFunction(String name,
      SingleIntraThreadSchedulingFunction... dependentFunctions) {
    super(name, dependentFunctions);
  }

  /**
   * Template method in place of {@link SingleIntraThreadSchedulingFunction#apply(Task, TaskIndexer, double[][])}, which automatically enforces
   * caching.
   *
   * @param task
   * @param indexer
   * @param features
   * @return
   */
  protected abstract double applyWithCachingSupport(Task task, TaskIndexer indexer,
      double[][] features);

  @Override
  public final double apply(Task task, TaskIndexer indexer, double[][] features) {
    if (caching) {
      if (cache[indexer.schedulerIndex(task)] < 0) {
        double result = applyWithCachingSupport(task, indexer, features);
        cache[indexer.schedulerIndex(task)] = result;
        return result;
      }
      return cache[indexer.schedulerIndex(task)];
    }
    return applyWithCachingSupport(task, indexer, features);
  }

  @Override
  public SingleIntraThreadSchedulingFunction enableCaching(int nTasks) {
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
  public void reset(int nTasks) {
    if (caching) {
      enableCaching(nTasks);
    }
  }

  @Override
  public boolean cachingEnabled() {
    return caching;
  }
}
