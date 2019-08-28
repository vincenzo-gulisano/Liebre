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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.Validate;

public class CombinedPriorityFunction implements MultiPriorityFunction {

  protected final SinglePriorityFunction[] functions;
  private final Feature[] requiredFeatures;
  private final String name;
  private boolean caching;

  public CombinedPriorityFunction(SinglePriorityFunction... functions) {
    Validate.notEmpty(functions, "At least one function is needed!");
    this.functions = functions;
    Set<Feature> functionFeatures = new HashSet<>();
    StringBuilder nameBuilder = new StringBuilder("Composite:");
    for (SinglePriorityFunction function : functions) {
      functionFeatures.addAll(Arrays.asList(function.requiredFeatures()));
      nameBuilder.append(function.name()).append(",");
    }
    this.name = nameBuilder.toString();
    this.requiredFeatures = functionFeatures.toArray(new Feature[0]);
  }

  @Override
  public void apply(Task task, double[][] features, double[] output) {
    Validate.isTrue(output.length == functions.length);
    for (int k = 0; k < output.length; k++) {
      output[k] = functions[k].apply(task, features);
    }
  }

  @Override
  public Feature[] requiredFeatures() {
    return requiredFeatures;
  }

  @Override
  public MultiPriorityFunction enableCaching(int nTasks) {
    this.caching = true;
    for (SinglePriorityFunction function : functions) {
      function.enableCaching(nTasks);
    }
    return this;
  }

  @Override
  public void clearCache() {
    for (SinglePriorityFunction function : functions) {
      function.clearCache();
    }
  }

  @Override
  public boolean cachingEnabled() {
    return caching;
  }

  @Override
  public int dimensions() {
    return functions.length;
  }

  @Override
  public boolean reverseOrder(int i) {
    return functions[i].reverseOrder();
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
