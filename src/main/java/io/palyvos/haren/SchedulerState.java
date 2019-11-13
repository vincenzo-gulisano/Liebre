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

import io.palyvos.haren.function.InterThreadSchedulingFunction;
import io.palyvos.haren.function.IntraThreadSchedulingFunction;
import io.palyvos.haren.function.VectorIntraThreadSchedulingFunction;
import io.palyvos.haren.function.VectorIntraThreadSchedulingFunctionComparator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** State object that contains information accessed by various scheduler comoponents. */
final class SchedulerState {

  private static final Logger LOG = LogManager.getLogger();
  // Features that might not be needed by any priority/deployment function
  // but are internally used by the scheduler
  private static final Feature[] SCHEDULER_REQUIRED_FEATURES = {Features.COMPONENT_TYPE};
  private final boolean[] updated;
  final double[][] taskFeatures;
  final double[][] priorities;
  final Comparator<Task> comparator;
  final String statisticsFolder;
  private final long[] lastUpdateTime;
  private VectorIntraThreadSchedulingFunction intraThreadSchedulingFunction;
  private final InterThreadSchedulingFunction interThreadSchedulingFunction;
  private final TaskIndexer indexer;
  private final Feature[] constantFeatures;
  // Non-constant features with at least one dependency
  private final Feature[] variableFeaturesWithDependencies;
  // Non-constant features with no dependencies
  private final Feature[] variableFeaturesNoDependencies;
  private final long[] barrierEnter;
  private final long[] barrierExit;
  private long roundEndTime;
  private long schedulingPeriod;
  private int batchSize;

  private final int nTasks;
  private final boolean priorityCaching;

  public SchedulerState(
      int nTasks,
      VectorIntraThreadSchedulingFunction intraThreadSchedulingFunction,
      InterThreadSchedulingFunction interThreadSchedulingFunction,
      boolean priorityCaching,
      String statisticsFolder,
      int nThreads,
      long schedulingPeriod,
      int batchSize) {
    Validate.isTrue(nTasks > 0);
    Validate.isTrue(nThreads > 0);
    Validate.notNull(intraThreadSchedulingFunction);
    Validate.notNull(interThreadSchedulingFunction);
    Validate.notBlank(statisticsFolder);
    // Init variables
    this.nTasks = nTasks;
    this.priorityCaching = priorityCaching;
    setSchedulingPeriod(schedulingPeriod);
    setBatchSize(batchSize);
    setIntraThreadSchedulingFunction(intraThreadSchedulingFunction);
    this.statisticsFolder = statisticsFolder;
    this.interThreadSchedulingFunction = interThreadSchedulingFunction;
    // Init more complex state
    this.indexer = new ReorderingTaskIndexer(nTasks);
    this.updated = new boolean[nTasks];
    this.taskFeatures = new double[nTasks][Features.length()];
    this.lastUpdateTime = new long[nTasks];
    this.priorities = new double[nTasks][intraThreadSchedulingFunction.dimensions()];
    this.comparator =
        new VectorIntraThreadSchedulingFunctionComparator(
            intraThreadSchedulingFunction, priorities, indexer);
    this.barrierEnter = new long[nThreads];
    this.barrierExit = new long[nThreads];
    this.constantFeatures =
        getFeatures(
            intraThreadSchedulingFunction,
            interThreadSchedulingFunction,
            feature -> feature.isConstant());
    this.variableFeaturesWithDependencies =
        getFeatures(
            intraThreadSchedulingFunction,
            interThreadSchedulingFunction,
            feature -> !feature.isConstant() && feature.dependencies().length > 0);
    this.variableFeaturesNoDependencies =
        getFeatures(
            intraThreadSchedulingFunction,
            interThreadSchedulingFunction,
            feature -> !feature.isConstant() && feature.dependencies().length == 0);
    LOG.info("Constant Features: {}", Arrays.toString(constantFeatures));
    LOG.info(
        "Variable Features with dependencies: {}",
        Arrays.toString(variableFeaturesWithDependencies));
    LOG.info(
        "Variable Features without dependencies: {}",
        Arrays.toString(variableFeaturesNoDependencies));
  }

  private Feature[] getFeatures(
      IntraThreadSchedulingFunction intraThreadSchedulingFunction,
      InterThreadSchedulingFunction interThreadSchedulingFunction,
      Predicate<Feature> predicate) {
    Set<Feature> allFeatures = new HashSet<>();
    allFeatures.addAll(Arrays.asList(intraThreadSchedulingFunction.requiredFeatures()));
    allFeatures.addAll(Arrays.asList(interThreadSchedulingFunction.requiredFeatures()));
    allFeatures.addAll(Arrays.asList(SCHEDULER_REQUIRED_FEATURES));
    return allFeatures.stream().filter(predicate).toArray(Feature[]::new);
  }

  private Feature[] getFeatures(
      IntraThreadSchedulingFunction intraThreadSchedulingFunction,
      InterThreadSchedulingFunction interThreadSchedulingFunction) {
    return getFeatures(
        intraThreadSchedulingFunction, interThreadSchedulingFunction, feature -> true);
  }

  void markUpdated(Task task) {
    updated[indexer.schedulerIndex(task)] = true;
  }

  void markRun(Task task, long timestamp) {
    lastUpdateTime[indexer.schedulerIndex(task)] = timestamp;
    updated[indexer.schedulerIndex(task)] = true;
  }

  boolean resetUpdated(Task task) {
    boolean state = updated[indexer.schedulerIndex(task)];
    updated[indexer.schedulerIndex(task)] = false;
    return state;
  }

  boolean timeToUpdate(Task task, long timestamp, long updateLimitMillis) {
    return timestamp - lastUpdateTime[indexer.schedulerIndex(task)] > updateLimitMillis;
  }

  void init(List<Task> tasks) {
    //TODO: Call this in case of elasticity
    interThreadSchedulingFunction.init(tasks, indexer, taskFeatures);
  }

  Feature[] constantFeatures() {
    return constantFeatures;
  }

  Feature[] variableFeaturesWithDependencies() {
    return variableFeaturesWithDependencies;
  }

  Feature[] variableFeaturesNoDependencies() {
    return variableFeaturesNoDependencies;
  }

  VectorIntraThreadSchedulingFunction intraThreadSchedulingFunction() {
    return intraThreadSchedulingFunction;
  }

  InterThreadSchedulingFunction interThreadSchedulingFunction() {
    return interThreadSchedulingFunction;
  }

  void updateRoundEndTime() {
    this.roundEndTime = System.currentTimeMillis() + schedulingPeriod;
  }

  long remainingRoundTime() {
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
      min = Math.min(d, min);
      max = Math.max(d, max);
    }
    return max - min;
  }

  long schedulingPeriod() {
    return schedulingPeriod;
  }

  void setSchedulingPeriod(long schedulingPeriod) {
    Validate.isTrue(schedulingPeriod > 0);
    this.schedulingPeriod = schedulingPeriod;
  }

  int batchSize() {
    return batchSize;
  }

  void setBatchSize(int batchSize) {
    Validate.isTrue(batchSize > 0);
    this.batchSize = batchSize;
  }

  void setIntraThreadSchedulingFunction(
      VectorIntraThreadSchedulingFunction intraThreadSchedulingFunction) {
    this.intraThreadSchedulingFunction =
        (priorityCaching && !intraThreadSchedulingFunction.cachingEnabled())
            ? intraThreadSchedulingFunction.enableCaching(nTasks)
            : intraThreadSchedulingFunction;
    this.intraThreadSchedulingFunction = intraThreadSchedulingFunction;
  }

  public TaskIndexer indexer() {
    return indexer;
  }
}
