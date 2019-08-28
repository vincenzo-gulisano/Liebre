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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


public enum Features implements Feature {
  TOPOLOGICAL_ORDER(0, true),
  SELECTIVITY(1, false),
  COST(2, false),
  HEAD_ARRIVAL_TIME(3, false),
  AVERAGE_ARRIVAL_TIME(4, false),
  COMPONENT_TYPE(5, true),
  RATE(6, false),
  USER_PRIORITY(7, true),
  INPUT_QUEUE_SIZE(8, false),
  OUTPUT_QUEUE_SIZE(9, false);

  public static final Map<Feature, FeatureDependency[]> dependencies;
  private static final FeatureDependency[] NO_DEPENDENCIES = new FeatureDependency[0];

  static {
    Map<Feature, FeatureDependency[]> deps = new HashMap<>();
    deps.put(HEAD_ARRIVAL_TIME,
        new FeatureDependency[]{FeatureDependency.of(HEAD_ARRIVAL_TIME),
            FeatureDependency.of(AVERAGE_ARRIVAL_TIME)});
    deps.put(AVERAGE_ARRIVAL_TIME,
        new FeatureDependency[]{FeatureDependency.of(HEAD_ARRIVAL_TIME),
            FeatureDependency.of(AVERAGE_ARRIVAL_TIME)});
    deps.put(INPUT_QUEUE_SIZE,
        new FeatureDependency[]{FeatureDependency.of(INPUT_QUEUE_SIZE, TaskDependency.DOWNSTREAM),
        });
    deps.put(OUTPUT_QUEUE_SIZE,
        new FeatureDependency[]{FeatureDependency.of(INPUT_QUEUE_SIZE, TaskDependency.DOWNSTREAM),
        });
    dependencies = Collections.unmodifiableMap(deps);
  }

  private final int index;
  private final boolean constant;

  Features(int index, boolean constant) {
    this.index = index;
    this.constant = constant;
  }

  public static int length() {
    return Features.values().length;
  }

  public static double[] createArray() {
    return new double[Features.length()];
  }

  @Override
  public int index() {
    return index;
  }

  @Override
  public boolean isConstant() {
    return constant;
  }

  @Override
  public double get(Task task, double[][] features) {
    return features[task.getIndex()][index];
  }

  @Override
  public FeatureDependency[] dependencies() {
    return dependencies.getOrDefault(this, NO_DEPENDENCIES);
  }
}
