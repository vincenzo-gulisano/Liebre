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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

//TODO: Constant vs changing feature
public enum Feature {
  TOPOLOGICAL_ORDER(0),
  SELECTIVITY(1),
  COST(2),
  HEAD_ARRIVAL_TIME(3),
  AVERAGE_ARRIVAL_TIME(4),
  COMPONENT_TYPE(5);

  public static final Map<Feature, FeatureDependency[]> dependencies;

  static {
    Map<Feature, FeatureDependency[]> deps = new HashMap<>();
    deps.put(HEAD_ARRIVAL_TIME,
        new FeatureDependency[]{FeatureDependency.of(HEAD_ARRIVAL_TIME),
            FeatureDependency.of(AVERAGE_ARRIVAL_TIME)});
    deps.put(AVERAGE_ARRIVAL_TIME,
        new FeatureDependency[]{FeatureDependency.of(HEAD_ARRIVAL_TIME),
            FeatureDependency.of(AVERAGE_ARRIVAL_TIME)});
    dependencies = Collections.unmodifiableMap(deps);
  }

  private static final FeatureDependency[] NO_DEPENDENCIES = new FeatureDependency[0];

  private final int index;

  Feature(int index) {
    this.index = index;
  }

  public int index() {
    return index;
  }

  public double get(Task task, double[][] features) {
    return features[task.getIndex()][index];
  }

  public FeatureDependency[] dependencies() {
    return dependencies.getOrDefault(this, NO_DEPENDENCIES);
  }

  public static int length() {
    return Feature.values().length;
  }

  public static double[] createArray() {
    return new double[Feature.length()];
  }
}
