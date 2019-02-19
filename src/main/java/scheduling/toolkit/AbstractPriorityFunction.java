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

public abstract class AbstractPriorityFunction implements SinglePriorityFunction {

  protected final SinglePriorityFunction[] dependentFunctions;
  private final Feature[] requiredFeatures;
  private final String name;

  public AbstractPriorityFunction(String name, Feature... requiredFeatures) {
    Validate.notEmpty(requiredFeatures, "Priority function has not features!");
    Validate.notEmpty(name);
    this.requiredFeatures = requiredFeatures;
    this.dependentFunctions = new SinglePriorityFunction[0];
    this.name = name;
  }

  public AbstractPriorityFunction(String name, SinglePriorityFunction... dependentFunctions) {
    Validate.notEmpty(name);
    Validate.notEmpty(dependentFunctions, "Priority function depends on no other function!");
    Set<Feature> features = new HashSet<>();
    for (SinglePriorityFunction function : dependentFunctions) {
      features.addAll(Arrays.asList(function.requiredFeatures()));
    }
    this.requiredFeatures = features.toArray(new Feature[0]);
    this.dependentFunctions = dependentFunctions;
    this.name = name;
  }

  @Override
  public SinglePriorityFunction enableCaching(int nTasks) {
    for (SinglePriorityFunction function : dependentFunctions) {
      function.enableCaching(nTasks);
    }
    return this;
  }

  @Override
  public void clearCache() {
    for (SinglePriorityFunction function : dependentFunctions) {
      function.clearCache();
    }
  }

  @Override
  public final Feature[] requiredFeatures() {
    return requiredFeatures.clone();
  }

  public final String name() {
    return name;
  }

}
