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

  private int taskCapacity;
  // Scheduler parameters
  private long schedulingPeriod;
  private int batchSize;
  private final boolean priorityCaching;
  // Features that might not be needed by any priority/deployment function
  // but are internally used by the scheduler
  private static final Feature[] SCHEDULER_REQUIRED_FEATURES = {Features.COMPONENT_TYPE};
  private final Feature[] constantFeatures;
  // Non-constant features with at least one dependency
  private final Feature[] variableFeaturesWithDependencies;
  // Non-constant features with no dependencies
  private final Feature[] variableFeaturesNoDependencies;
  // Task state
  private boolean[] updated;
  private long[] lastUpdateTime;
  double[][] taskFeatures;
  double[][] priorities;
  // Thread state
  private final long[] barrierEnter;
  private final long[] barrierExit;
  // Other
  private VectorIntraThreadSchedulingFunction intraThreadSchedulingFunction;
  private final InterThreadSchedulingFunction interThreadSchedulingFunction;
  private final TaskIndexer indexer;
  final String statisticsFolder;
  Comparator<Task> comparator;
  private long roundEndTime;

  public SchedulerState(
      int taskCapacity,
      VectorIntraThreadSchedulingFunction intraThreadSchedulingFunction,
      InterThreadSchedulingFunction interThreadSchedulingFunction,
      boolean priorityCaching,
      String statisticsFolder,
      int nThreads,
      long schedulingPeriod,
      int batchSize) {
    Validate.isTrue(taskCapacity > 0);
    Validate.isTrue(nThreads > 0);
    Validate.notNull(intraThreadSchedulingFunction);
    Validate.notNull(interThreadSchedulingFunction);
    Validate.notBlank(statisticsFolder);
    // Init complex state
    this.indexer = new ReorderingTaskIndexer(taskCapacity);
    this.updated = new boolean[taskCapacity];
    this.taskFeatures = new double[taskCapacity][Features.length()];
    this.lastUpdateTime = new long[taskCapacity];
    this.priorities = new double[taskCapacity][intraThreadSchedulingFunction.dimensions()];
    this.barrierEnter = new long[nThreads];
    this.barrierExit = new long[nThreads];
    // Init variables
    this.taskCapacity = taskCapacity;
    this.priorityCaching = priorityCaching;
    setSchedulingPeriod(schedulingPeriod);
    setBatchSize(batchSize);
    setIntraThreadSchedulingFunction(intraThreadSchedulingFunction);
    this.statisticsFolder = statisticsFolder;
    this.interThreadSchedulingFunction = interThreadSchedulingFunction;
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

  void resetSchedulingFunctions(List<Task> tasks) {
    interThreadSchedulingFunction().reset(tasks, taskCapacity, indexer, taskFeatures);
    intraThreadSchedulingFunction().reset(taskCapacity);
    resetIntraThreadPriorityComparator();
  }

  void setIntraThreadSchedulingFunction(
      VectorIntraThreadSchedulingFunction intraThreadSchedulingFunction) {
    this.intraThreadSchedulingFunction =
        (priorityCaching && !intraThreadSchedulingFunction.cachingEnabled())
            ? intraThreadSchedulingFunction.enableCaching(taskCapacity)
            : intraThreadSchedulingFunction;
    resetIntraThreadPriorityComparator();
  }

  void resetIntraThreadPriorityComparator() {
    this.comparator =
        new VectorIntraThreadSchedulingFunctionComparator(
            this.intraThreadSchedulingFunction, priorities, indexer);
  }

  TaskIndexer indexer() {
    return indexer;
  }

  void unregisterTasks(List<Task> tasksToRemove) {
    for (Task task : tasksToRemove) {
      clearTaskState(task);
    }
    indexer.unregisterTasks(tasksToRemove);
  }

  private void clearTaskState(Task task) {
    LOG.debug("Clearing state for removed task {}", task);
    int taskIndex = indexer.schedulerIndex(task);
    updated[taskIndex] = false;
    double[] featureVector = taskFeatures[taskIndex];
    for (int i = 0; i < featureVector.length; i++) {
      featureVector[i] = 0;
    }
    double[] priorityVector = priorities[taskIndex];
    for (int i = 0; i < priorityVector.length; i++) {
      priorityVector[i] = 0;
    }
    lastUpdateTime[taskIndex] = 0;
  }

  public void registerTasks(List<Task> tasksToAdd) {
    indexer.registerTasks(tasksToAdd);
    int newNumberOfTasks = indexer.indexedTasks();
    if (newNumberOfTasks > taskCapacity) {
      resizeTaskState(newNumberOfTasks);
      // Note: In the current implementation, the number of tasks never decreases
      // so we only do this step for additional tasks
      taskCapacity = newNumberOfTasks;
    }
  }

  private void resizeTaskState(int newNumberOfTasks) {
    LOG.info("Resizing task state from {} to {}...", taskCapacity, newNumberOfTasks);
    boolean[] newUpdated = new boolean[newNumberOfTasks];
    System.arraycopy(updated, 0, newUpdated, 0, updated.length);
    this.updated = newUpdated;
    long[] newLastUpdateTime = new long[newNumberOfTasks];
    System.arraycopy(lastUpdateTime, 0, newLastUpdateTime, 0, lastUpdateTime.length);
    this.lastUpdateTime = newLastUpdateTime;
    this.taskFeatures = enlarge2DArray(taskFeatures, newNumberOfTasks, Features.length());
    this.priorities =
        enlarge2DArray(priorities, newNumberOfTasks, intraThreadSchedulingFunction.dimensions());
  }

  private double[][] enlarge2DArray(double[][] source, int destRows, int destCols) {
    int rows = source.length;
    int cols = source[0].length;
    Validate.isTrue(destRows >= rows);
    Validate.isTrue(destCols >= cols);
    double[][] dest = new double[destRows][destCols];
    for (int i = 0; i < rows; i++) {
      System.arraycopy(source[i], 0, dest[i], 0, cols);
    }
    return dest;
  }
}
