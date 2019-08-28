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
import java.util.List;
import org.apache.commons.lang3.Validate;

public abstract class AbstractInterThreadSchedulingFunction implements
    InterThreadSchedulingFunction {

  protected double[][] features;
  protected List<Task> tasks;
  private final Features[] requiredFeatures;
  private final String name;

  protected AbstractInterThreadSchedulingFunction(String name, Features... requiredFeatures) {
    Validate.notEmpty(name);
    this.requiredFeatures = requiredFeatures;
    this.name = name;
  }

  @Override
  public void init(List<Task> tasks, double[][] features) {
    this.features = features;
    this.tasks = tasks;
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
