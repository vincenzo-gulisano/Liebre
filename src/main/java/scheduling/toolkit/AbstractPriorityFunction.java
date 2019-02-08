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

  public AbstractPriorityFunction(Feature... features) {
    Validate.notEmpty(features, "Priority function has not features!");
    this.features = features;
  }

  public AbstractPriorityFunction(PriorityFunction ...dependentFunctions) {
    Set<Feature> features = new HashSet<>();
    for (PriorityFunction function : dependentFunctions) {
      features.addAll(Arrays.asList(function.features()));
    }
    this.features = features.toArray(new Feature[0]);
  }

  @Override
  public final Feature[] features() {
    return features.clone();
  }

}
