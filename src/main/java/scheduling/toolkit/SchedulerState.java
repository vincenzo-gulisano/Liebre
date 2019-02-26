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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class SchedulerState {


  private static final Logger LOG = LogManager.getLogger();
  // Features that might not be needed by any priority/deployment function
  // but are internally used by the scheduler
  private static final Feature[] SCHEDULER_REQUIRED_FEATURES = {Feature.COMPONENT_TYPE};
  final AtomicBoolean[] updated;
  final double[][] taskFeatures;
  final double[][] priorities;
  final Comparator<Task> comparator;
  final String statisticsFolder;
  private final long[] lastUpdateTime;
  private final MultiPriorityFunction priorityFunction;
  private final DeploymentFunction deploymentFunction;
  private final Feature[] constantFeatures;
  //Non-constant features with at least one dependency
  private final Feature[] variableFeaturesWithDependencies;
  //Non-constant features with no dependencies
  private final Feature[] variableFeaturesNoDependencies;

  public SchedulerState(int nTasks, MultiPriorityFunction priorityFunction,
      DeploymentFunction deploymentFunction,
      boolean priorityCachning,
      String statisticsFolder) {
    this.priorityFunction = priorityCachning ? priorityFunction.enableCaching(nTasks) :
        priorityFunction;
    this.deploymentFunction = deploymentFunction;
    this.updated = new AtomicBoolean[nTasks];
    this.taskFeatures = new double[nTasks][Feature.length()];
    this.lastUpdateTime = new long[nTasks];
    this.priorities = new double[nTasks][priorityFunction.dimensions()];
    this.comparator = new MultiPriorityComparator(priorityFunction, priorities);
    for (int i = 0; i < updated.length; i++) {
      updated[i] = new AtomicBoolean(false);
    }
    this.statisticsFolder = statisticsFolder;
    this.constantFeatures = getFeatures(priorityFunction, deploymentFunction,
        feature -> feature.isConstant());
    this.variableFeaturesWithDependencies = getFeatures(priorityFunction, deploymentFunction,
        feature -> !feature.isConstant() && feature.dependencies().length > 0);
    this.variableFeaturesNoDependencies = getFeatures(priorityFunction, deploymentFunction,
        feature -> !feature.isConstant() && feature.dependencies().length == 0);
    LOG.info("Constant Features: {}", Arrays.toString(constantFeatures));
    LOG.info("Variable Features with dependencies: {}", Arrays.toString(
        variableFeaturesWithDependencies));
    LOG.info("Variable Features without dependencies: {}",
        Arrays.toString(variableFeaturesNoDependencies));
  }

  private Feature[] getFeatures(PriorityFunction priorityFunction,
      DeploymentFunction deploymentFunction, Predicate<Feature> predicate) {
    Set<Feature> allFeatures = new HashSet<>();
    allFeatures.addAll(Arrays.asList(priorityFunction.requiredFeatures()));
    allFeatures.addAll(Arrays.asList(deploymentFunction.requiredFeatures()));
    allFeatures.addAll(Arrays.asList(SCHEDULER_REQUIRED_FEATURES));
    return allFeatures.stream().filter(predicate).toArray(Feature[]::new);
  }

  private Feature[] getFeatures(PriorityFunction priorityFunction,
      DeploymentFunction deploymentFunction) {
    return getFeatures(priorityFunction, deploymentFunction, feature -> true);
  }

  void markUpdate(Task task, long timestamp) {
    lastUpdateTime[task.getIndex()] = timestamp;
  }

  boolean timeToUpdate(Task task, long timestamp, long updateLimitMillis) {
    return timestamp - lastUpdateTime[task.getIndex()] > updateLimitMillis;
  }

  void init(List<Task> tasks) {
    deploymentFunction.init(tasks, taskFeatures);
  }

  Feature[] constantFeatures() {
    return constantFeatures;
  }

  public Feature[] variableFeaturesWithDependencies() {
    return variableFeaturesWithDependencies;
  }

  public Feature[] variableFeaturesNoDependencies() {
    return variableFeaturesNoDependencies;
  }

  MultiPriorityFunction priorityFunction() {
    return priorityFunction;
  }

  DeploymentFunction deploymentFunction() {
    return deploymentFunction;
  }
}
