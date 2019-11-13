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

import io.palyvos.haren.Feature;
import io.palyvos.haren.Task;
import io.palyvos.haren.TaskIndexer;

/**
 * Decorator that reverses a {@link SingleIntraThreadSchedulingFunction}.
 */
class ReverseIntraThreadSchedulingFunction implements
    SingleIntraThreadSchedulingFunction {

  private static final double PREVENT_DIV_ZERO = Math.pow(10, -10);
  private final SingleIntraThreadSchedulingFunction original;

  ReverseIntraThreadSchedulingFunction(SingleIntraThreadSchedulingFunction original) {
    this.original = original;
  }

  @Override
  public double apply(Task task, TaskIndexer indexer, double[][] features) {
    return 1 / (original.apply(task, indexer, features) + PREVENT_DIV_ZERO);
  }

  @Override
  public Feature[] requiredFeatures() {
    return original.requiredFeatures();
  }

  @Override
  public SingleIntraThreadSchedulingFunction enableCaching(int nTasks) {
    return original.enableCaching(nTasks);
  }

  @Override
  public void clearCache() {
    original.clearCache();
  }

  @Override
  public void reset(int nTasks) {
    original.reset(nTasks);
  }

  @Override
  public boolean cachingEnabled() {
    return original.cachingEnabled();
  }

  @Override
  public String name() {
    return original.name() + "_reverse";
  }
}
