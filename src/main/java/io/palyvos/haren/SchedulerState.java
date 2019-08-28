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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class SchedulerState {


  private static final Logger LOG = LogManager.getLogger();
  // Features that might not be needed by any priority/deployment function
  // but are internally used by the scheduler
  private static final Feature[] SCHEDULER_REQUIRED_FEATURES = {Feature.COMPONENT_TYPE};
  private final boolean[] updated;
  final double[][] taskFeatures;
  final double[][] priorities;
  final Comparator<Task> comparator;
  final String statisticsFolder;
  private final long[] lastUpdateTime;
  private final MultiIntraThreadSchedulingFunction priorityFunction;
  private final InterThreadSchedulingFunction interThreadSchedulingFunction;
  private final Feature[] constantFeatures;
  //Non-constant features with at least one dependency
  private final Feature[] variableFeaturesWithDependencies;
  //Non-constant features with no dependencies
  private final Feature[] variableFeaturesNoDependencies;
  private final long[] barrierEnter;
  private final long[] barrierExit;
  private long roundEndTime;
  private final long schedulingPeriod;

  public SchedulerState(int nTasks, MultiIntraThreadSchedulingFunction priorityFunction,
      InterThreadSchedulingFunction interThreadSchedulingFunction,
      boolean priorityCachning,
      String statisticsFolder, int nThreads, long schedulingPeriod) {
    this.priorityFunction = priorityCachning ? priorityFunction.enableCaching(nTasks) :
        priorityFunction;
    this.interThreadSchedulingFunction = interThreadSchedulingFunction;
    this.updated = new boolean[nTasks];
    this.taskFeatures = new double[nTasks][Feature.length()];
    this.lastUpdateTime = new long[nTasks];
    this.schedulingPeriod = schedulingPeriod;
    this.priorities = new double[nTasks][priorityFunction.dimensions()];
    this.comparator = new MultiPriorityComparator(priorityFunction, priorities);
    this.barrierEnter = new long[nThreads];
    this.barrierExit = new long[nThreads];
    this.statisticsFolder = statisticsFolder;
    this.constantFeatures = getFeatures(priorityFunction, interThreadSchedulingFunction,
        feature -> feature.isConstant());
    this.variableFeaturesWithDependencies = getFeatures(priorityFunction,
        interThreadSchedulingFunction,
        feature -> !feature.isConstant() && feature.dependencies().length > 0);
    this.variableFeaturesNoDependencies = getFeatures(priorityFunction,
        interThreadSchedulingFunction,
        feature -> !feature.isConstant() && feature.dependencies().length == 0);
    LOG.info("Constant Features: {}", Arrays.toString(constantFeatures));
    LOG.info("Variable Features with dependencies: {}", Arrays.toString(
        variableFeaturesWithDependencies));
    LOG.info("Variable Features without dependencies: {}",
        Arrays.toString(variableFeaturesNoDependencies));
  }

  private Feature[] getFeatures(IntraThreadSchedulingFunction intraThreadSchedulingFunction,
      InterThreadSchedulingFunction interThreadSchedulingFunction, Predicate<Feature> predicate) {
    Set<Feature> allFeatures = new HashSet<>();
    allFeatures.addAll(Arrays.asList(intraThreadSchedulingFunction.requiredFeatures()));
    allFeatures.addAll(Arrays.asList(interThreadSchedulingFunction.requiredFeatures()));
    allFeatures.addAll(Arrays.asList(SCHEDULER_REQUIRED_FEATURES));
    return allFeatures.stream().filter(predicate).toArray(Feature[]::new);
  }

  private Feature[] getFeatures(IntraThreadSchedulingFunction intraThreadSchedulingFunction,
      InterThreadSchedulingFunction interThreadSchedulingFunction) {
    return getFeatures(intraThreadSchedulingFunction, interThreadSchedulingFunction, feature -> true);
  }

  void markUpdated(Task task) {
    updated[task.getIndex()] = true;
  }

  void markRun(Task task, long timestamp) {
    lastUpdateTime[task.getIndex()] = timestamp;
    updated[task.getIndex()] = true;
  }

  boolean resetUpdated(Task task) {
    boolean state = updated[task.getIndex()];
    updated[task.getIndex()] = false;
    return state;
  }

  boolean timeToUpdate(Task task, long timestamp, long updateLimitMillis) {
    return timestamp - lastUpdateTime[task.getIndex()] > updateLimitMillis;
  }

  void init(List<Task> tasks) {
    interThreadSchedulingFunction.init(tasks, taskFeatures);
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

  MultiIntraThreadSchedulingFunction priorityFunction() {
    return priorityFunction;
  }

  InterThreadSchedulingFunction deploymentFunction() {
    return interThreadSchedulingFunction;
  }

  void updateRoundEndTime() {
   this.roundEndTime = System.currentTimeMillis() + schedulingPeriod;
  }

  public long remainingRoundTime() {
    return roundEndTime - System.currentTimeMillis();
  }

  void recordBarrierEnter(int executorIndex, long duration) {
    barrierEnter[executorIndex] = duration;
  }

  void recordBarrierExit(int executorIndex, long duration) {
    barrierExit[executorIndex] = duration;
  }

  long barrierEnterVariance() {
    return variance(barrierEnter);
  }

  long barrierExitVariance() {
    return variance(barrierExit);
  }

  private long variance(long[] data) {
    long min = data[0];
    long max = data[0];
    for (long d : data) {
      min = Math.min(d, min) ;
      max = Math.max(d, max);
    }
    return max - min;
  }
}
