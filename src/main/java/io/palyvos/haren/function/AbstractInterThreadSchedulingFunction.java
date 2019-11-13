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
import io.palyvos.haren.Features;
import io.palyvos.haren.Task;
import io.palyvos.haren.TaskIndexer;
import java.util.List;
import org.apache.commons.lang3.Validate;

/**
 * Abstract implementation of {@link InterThreadSchedulingFunction}, handling basic functionality.
 */
public abstract class AbstractInterThreadSchedulingFunction
    implements InterThreadSchedulingFunction {

  private final Features[] requiredFeatures;
  private final String name;
  protected double[][] features;
  protected List<Task> tasks;
  protected TaskIndexer indexer;

  /**
   * Construct.
   *
   * @param name The function's name.
   * @param requiredFeatures The features used by this function.
   */
  protected AbstractInterThreadSchedulingFunction(String name, Features... requiredFeatures) {
    Validate.notEmpty(name);
    this.requiredFeatures = requiredFeatures;
    this.name = name;
  }

  @Override
  public void reset(List<Task> tasks, TaskIndexer indexer, double[][] features) {
    this.features = features;
    this.tasks = tasks;
    this.indexer = indexer;
  }

  public Feature[] requiredFeatures() {
    return requiredFeatures.clone();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public String toString() {
    return name();
  }
}
