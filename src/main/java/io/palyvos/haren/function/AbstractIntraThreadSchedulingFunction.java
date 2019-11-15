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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.lang3.Validate;

/**
 * Abstract implementation of {@link SingleIntraThreadSchedulingFunction}, handling basic
 * functionality.
 */
public abstract class AbstractIntraThreadSchedulingFunction implements
    SingleIntraThreadSchedulingFunction {

  protected final SingleIntraThreadSchedulingFunction[] dependentFunctions;
  private final Feature[] requiredFeatures;
  private final String name;
  private boolean caching;

  /**
   * Construct.
   *
   * @param name The function's name.
   * @param requiredFeatures The features used by this function.
   */
  protected AbstractIntraThreadSchedulingFunction(String name, Features... requiredFeatures) {
    Validate.notEmpty(requiredFeatures, "Priority function has no features!");
    Validate.notEmpty(name);
    this.requiredFeatures = requiredFeatures;
    this.dependentFunctions = new SingleIntraThreadSchedulingFunction[0];
    this.name = name;
  }

  /**
   * Construct.
   *
   * @param name The functions name.
   * @param dependentFunctions Other {@link SingleIntraThreadSchedulingFunction}s used by this
   *     function.
   */
  protected AbstractIntraThreadSchedulingFunction(String name,
      SingleIntraThreadSchedulingFunction... dependentFunctions) {
    Validate.notEmpty(name);
    Validate.notEmpty(dependentFunctions, "Priority function depends on no other function!");
    Set<Feature> features = new HashSet<>();
    for (SingleIntraThreadSchedulingFunction function : dependentFunctions) {
      features.addAll(Arrays.asList(function.requiredFeatures()));
    }
    this.requiredFeatures = features.toArray(new Feature[0]);
    this.dependentFunctions = dependentFunctions;
    this.name = name;
  }

  @Override
  public SingleIntraThreadSchedulingFunction enableCaching(int nTasks) {
    caching = true;
    for (SingleIntraThreadSchedulingFunction function : dependentFunctions) {
      function.enableCaching(nTasks);
    }
    return this;
  }

  @Override
  public void clearCache() {
    for (SingleIntraThreadSchedulingFunction function : dependentFunctions) {
      function.clearCache();
    }
  }

  @Override
  public boolean cachingEnabled() {
    return caching;
  }

  @Override
  public final Feature[] requiredFeatures() {
    return requiredFeatures.clone();
  }

  @Override
  public final String name() {
    return name;
  }

  @Override
  public String toString() {
    return name();
  }
}