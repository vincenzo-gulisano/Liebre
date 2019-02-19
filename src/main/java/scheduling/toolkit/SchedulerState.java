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
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

public final class SchedulerState {

  final AtomicBoolean[] updated;
  final double[][] taskFeatures;
  final String statisticsFolder;
  private final MultiPriorityFunction priorityFunction;
  private final DeploymentFunction deploymentFunction;
  private final Feature[] requiredFeatures;

  public SchedulerState(int nTasks, MultiPriorityFunction priorityFunction,
      DeploymentFunction deploymentFunction,
      boolean priorityCachning,
      String statisticsFolder) {
    updated = new AtomicBoolean[nTasks];
    taskFeatures = new double[nTasks][Feature.length()];
    for (int i = 0; i < updated.length; i++) {
      updated[i] = new AtomicBoolean(false);
    }
    this.priorityFunction = priorityCachning ? priorityFunction.enableCaching(nTasks) :
        priorityFunction;
    this.deploymentFunction = deploymentFunction;
    this.statisticsFolder = statisticsFolder;
    this.requiredFeatures = getAllFeatures(priorityFunction, deploymentFunction);
  }

  private Feature[] getAllFeatures(PriorityFunction priorityFunction, DeploymentFunction deploymentFunction) {
    Set<Feature> allFeatures = new HashSet<>();
    allFeatures.addAll(Arrays.asList(priorityFunction.requiredFeatures()));
    allFeatures.addAll(Arrays.asList(deploymentFunction.requiredFeatures()));
    return allFeatures.toArray(new Feature[0]);
  }

  void init(List<Task> tasks) {
    deploymentFunction.init(tasks, taskFeatures);
  }

  /**
   * Required features for the priority and deployment functions.
   *
   * @return An array of all the (unique) required features.
   */
  Feature[] requiredFeatures() {
    return requiredFeatures;
  }

  MultiPriorityFunction priorityFunction() {
    return priorityFunction;
  }

  DeploymentFunction deploymentFunction() {
    return deploymentFunction;
  }
}
