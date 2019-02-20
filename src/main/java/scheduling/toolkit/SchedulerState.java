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
import java.util.function.Predicate;

public final class SchedulerState {

  // Features that might not be needed by any priority/deployment function
  // but are internally used by the scheduler
  private static final Feature[] SCHEDULER_REQUIRED_FEATURES = {Feature.COMPONENT_TYPE};
  final AtomicBoolean[] updated;
  final double[][] taskFeatures;
  final String statisticsFolder;
  private final MultiPriorityFunction priorityFunction;
  private final DeploymentFunction deploymentFunction;
  private final Feature[] allRequiredFeatures;
  private final Feature[] changingRequiredFeatures;

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
    this.allRequiredFeatures = getAllFeatures(priorityFunction, deploymentFunction);
    this.changingRequiredFeatures = getAllFeatures(priorityFunction, deploymentFunction,
        feature -> !feature.isConstant());
  }

  private Feature[] getAllFeatures(PriorityFunction priorityFunction,
      DeploymentFunction deploymentFunction, Predicate<Feature> predicate) {
    Set<Feature> allFeatures = new HashSet<>();
    allFeatures.addAll(Arrays.asList(priorityFunction.requiredFeatures()));
    allFeatures.addAll(Arrays.asList(deploymentFunction.requiredFeatures()));
    allFeatures.addAll(Arrays.asList(SCHEDULER_REQUIRED_FEATURES));
    return allFeatures.stream().filter(predicate).toArray(Feature[]::new);
  }

  private Feature[] getAllFeatures(PriorityFunction priorityFunction,
      DeploymentFunction deploymentFunction) {
    return getAllFeatures(priorityFunction, deploymentFunction, feature -> true);
  }

  void init(List<Task> tasks) {
    deploymentFunction.init(tasks, taskFeatures);
  }

  /**
   * Required features for the priority and deployment functions.
   *
   * @param includingConstant Choice of whether to include the non-changing features or not
   * @return An array of all the (unique) required features.
   */
  Feature[] requiredFeatures(boolean includingConstant) {
    return includingConstant ? allRequiredFeatures : changingRequiredFeatures;
  }

  MultiPriorityFunction priorityFunction() {
    return priorityFunction;
  }

  DeploymentFunction deploymentFunction() {
    return deploymentFunction;
  }
}
