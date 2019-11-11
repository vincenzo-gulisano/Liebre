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

import common.statistic.AbstractCummulativeStatistic;
import common.statistic.MeterStatistic;
import common.util.StatisticPath;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import net.openhft.affinity.Affinity;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class encompassing the exectuon logic of every worker thread of {@link HarenScheduler}.
 */
public abstract class AbstractExecutor implements Runnable {

  public static final String EXECUTOR_STATISTIC_TIME = "executor-time";
  private static final String EXECUTED_TASK_COUNTS = "executed-tasks";
  private static final int BACKOFF_MIN_MILLIS = 1;
  private static final AtomicInteger indexGenerator = new AtomicInteger();
  private static final Logger LOG = LogManager.getLogger();
  private static final int BACKOFF_RETRIES = 3;
  private static final int TASK_UPDATE_LIMIT_FACTOR = 2;
  protected final CyclicBarrier barrier;
  protected final SchedulerState state;
  private final int index;
  private final Set<Integer> runTasks = new HashSet<>();
  private final Set<TaskDependency> taskDependencies = new HashSet<>();
  private final SchedulerBackoff backoff;
  private final AbstractCummulativeStatistic updateTime;
  private final AbstractCummulativeStatistic waitTime;
  private final AbstractCummulativeStatistic sortTime;
  private final AbstractCummulativeStatistic priorityTime;
  private final AbstractCummulativeStatistic otherTime;
  private final AbstractCummulativeStatistic markedTasks;
  private final AbstractCummulativeStatistic laggingTasks;
  private final AbstractCummulativeStatistic dependentTasks;
  private final AbstractCummulativeStatistic backoffCalls;
  private final int cpuId;
  protected volatile List<Task> executorTasks = Collections.emptyList();

  public AbstractExecutor(SchedulerState state, CyclicBarrier barrier) {
    this(state, -1, barrier);
  }

  public AbstractExecutor(
      SchedulerState state, int cpuId, CyclicBarrier barrier) {
    this.barrier = barrier;
    this.state = state;
    this.backoff = new SchedulerBackoff(BACKOFF_MIN_MILLIS,
        state.schedulingPeriod() / 10,
        BACKOFF_RETRIES);
    this.index = indexGenerator.getAndIncrement();
    this.cpuId = cpuId;
    this.updateTime = new MeterStatistic(
        StatisticPath.get(state.statisticsFolder, String.format(
            "Update-Action-Executor-%d", index),
            EXECUTOR_STATISTIC_TIME),
        false);
    this.sortTime = new MeterStatistic(
        StatisticPath.get(state.statisticsFolder, String.format(
            "Sort-Tasks-Executor-%d", index),
            EXECUTOR_STATISTIC_TIME), false);
    this.priorityTime = new MeterStatistic(
        StatisticPath.get(state.statisticsFolder, String.format(
            "Priority-Update-Executor-%d", index),
            EXECUTOR_STATISTIC_TIME), false);
    this.otherTime = new MeterStatistic(
        StatisticPath.get(state.statisticsFolder, String.format(
            "Other-Overheads-Executor-%d", index),
            EXECUTOR_STATISTIC_TIME), false);
    this.waitTime = new MeterStatistic(StatisticPath.get(state.statisticsFolder, String.format(
        "Wait-Barrier-Executor-%d", index), EXECUTOR_STATISTIC_TIME),
        false);
    this.markedTasks = new MeterStatistic(StatisticPath.get(state.statisticsFolder,
        String.format("Marked-Tasks-Executor-%d", index), EXECUTED_TASK_COUNTS),
        false);
    this.laggingTasks = new MeterStatistic(StatisticPath.get(state.statisticsFolder,
        String.format("Lagging-Tasks-Executor-%d", index), EXECUTED_TASK_COUNTS),
        false);
    this.dependentTasks = new MeterStatistic(StatisticPath.get(state.statisticsFolder,
        String.format("Dependent-Tasks-Executor-%d", index), EXECUTED_TASK_COUNTS),
        false);
    this.backoffCalls = new MeterStatistic(StatisticPath.get(state.statisticsFolder,
        String.format("Backoff-Calls-Executor-%d", index), EXECUTED_TASK_COUNTS),
        false);
    initTaskDependencies(state.variableFeaturesWithDependencies());
  }

  private void enableStatistics() {
    updateTime.enable();
    waitTime.enable();
    sortTime.enable();
    priorityTime.enable();
    otherTime.enable();
    markedTasks.enable();
    laggingTasks.enable();
    dependentTasks.enable();
    backoffCalls.enable();
  }

  private void initTaskDependencies(Feature[] features) {
    // Merge feature dependencies, i.e. upstream, downstream, ...
    for (Feature feature : features) {
      for (FeatureDependency dependency : feature.dependencies()) {
        taskDependencies.addAll(Arrays.asList(dependency.dependencies));
      }
    }
  }

  public void setTasks(List<Task> tasks) {
    this.executorTasks = tasks;
  }

  @Override
  public void run() {
    if (cpuId > 0) {
      LOG.info("Setting affinity to CPU #{}", cpuId);
      Affinity.setAffinity(cpuId);
    }
    if (!updateTasks()) {
      return;
    }
    beginRound();
    while (!Thread.currentThread().isInterrupted()) {
      boolean didRun = runNextTask();
      adjustUtilization(didRun, state.remainingRoundTime());
      if (state.remainingRoundTime() <= 0) {
        if (!updateTasks()) {
          break;
        }
        beginRound();
      }
    }
    updateTime.disable();
    waitTime.disable();
    sortTime.disable();
    priorityTime.disable();
    otherTime.disable();
    markedTasks.disable();
    laggingTasks.disable();
    dependentTasks.disable();
    backoffCalls.disable();
  }

  /**
   * Update the start time of the new round, but count the sort time in the round time, to achieve
   * synchronized entry to the barrier between processing threads.
   *
   * @return The start time of the rounds.
   */
  private void beginRound() {
    calculatePriorities();
    sortTasks();
    long otherStart = System.currentTimeMillis();
    onRoundStart();
    otherTime.append(System.currentTimeMillis() - otherStart);
    runLaggingTasks();
    backoff.reset();
    if (LOG.getLevel() == Level.TRACE) {
      printTasks();
    }
  }

  private void calculatePriorities() {
    long start = System.currentTimeMillis();
    for (Task task : executorTasks) {
      state.intraThreadSchedulingFunction().apply(task, state.taskFeatures, state.priorities[task.getIndex()]);
    }
    priorityTime.append(System.currentTimeMillis() - start);
  }

  private void runLaggingTasks() {
    long timestamp = System.currentTimeMillis();
    for (int i = 0; i < executorTasks.size(); i++) {
      Task task = executorTasks.get(i);
      if (state.timeToUpdate(task, timestamp,
          TASK_UPDATE_LIMIT_FACTOR * state.schedulingPeriod())) {
        task.runFor(1);
        mark(task, i);
        laggingTasks.append(1);
      }
    }
  }

  private void adjustUtilization(boolean didRun, long remainingTime) {
    if (remainingTime <= 0) {
      return;
    }
    if (!didRun) {
      backoffCalls.append(1);
      backoff.backoff(remainingTime);
      return;
    }
    backoff.relax();
  }

  private boolean updateTasks() {
    try {
      markUpdated();
      long barrierEnterTime = System.currentTimeMillis();
      state.recordBarrierEnter(index, barrierEnterTime);
      barrier.await();
      long barrierExitTime = System.currentTimeMillis();
      waitTime.append(barrierExitTime - barrierEnterTime);
      state.recordBarrierExit(index, barrierExitTime);
      return true;
    } catch (InterruptedException | BrokenBarrierException e) {
      return false;
    }
  }

  private void sortTasks() {
    long startTime = System.currentTimeMillis();
    executorTasks.sort(state.comparator);
    sortTime.append(System.currentTimeMillis() - startTime);
  }

  private void markUpdated() {
    long markTime = System.currentTimeMillis();
    // Refresh features, for example those who are recorded as moving averages
    for (Task task : executorTasks) {
      task.refreshFeatures();
    }
    markedTasks.append(runTasks.size());
    int totalDependentCount = 0;
    for (int taskIndex : runTasks) {
      Task task = executorTasks.get(taskIndex);
      task.updateFeatures(state.variableFeaturesNoDependencies(),
          state.taskFeatures[task.getIndex()]);
      state.markRun(task, markTime);
      for (TaskDependency taskDependency : taskDependencies) {
        for (Task dependent : taskDependency.dependents(task)) {
          state.markUpdated(dependent);
          totalDependentCount++;
        }
      }
    }
    dependentTasks.append(totalDependentCount);
    runTasks.clear();
    updateTime.append(System.currentTimeMillis() - markTime);
  }

  /**
   * Mark the task with that has the given index in the <b>LOCAL task list</b> as executed. Do
   * NOT use {@link Task#getIndex()} in this function, except if it matches the local index
   * (which it usually does not).
   *
   * @param task The task to mark
   * @param localIndex The index of the task in executorTasks
   */
  protected final void mark(Task task, int localIndex) {
    runTasks.add(localIndex);
  }

  protected abstract boolean runNextTask();

  protected abstract void onRoundStart();

  private final void printTasks() {
    synchronized (AbstractExecutor.class) {
      LOG.info("-----Thread assignment-----");
      for (Task task : executorTasks) {
        LOG.info("[{}, {}] -> {}", task,
            Arrays.toString(state.priorities[task.getIndex()]),
            Arrays.toString(state.taskFeatures[task.getIndex()]));
      }
    }
  }

  @Override
  public String toString() {
    return "EXECUTOR: " + executorTasks + "\n";
  }
}
