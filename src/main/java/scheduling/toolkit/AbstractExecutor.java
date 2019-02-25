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

import common.statistic.AbstractCummulativeStatistic;
import common.statistic.CountStatistic;
import common.util.StatisticPath;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractExecutor implements Runnable {

  public static final String EXECUTOR_STATISTIC_TIME = "executor-time";
  private static final int BACKOFF_MIN_MILLIS = 1;
  private static final AtomicInteger indexGenerator = new AtomicInteger();
  private static final Logger LOG = LogManager.getLogger();
  private static final int BACKOFF_RETRIES = 3;
  protected final int batchSize;
  protected final CyclicBarrier barrier;
  protected final SchedulerState state;
  private final int index;
  private final long schedulingPeriod;
  private final Set<Integer> runTasks = new HashSet<>();
  private final Set<TaskDependency> taskDependencies = new HashSet<>();
  private final SchedulerBackoff backoff;
  private final AbstractCummulativeStatistic updateTime;
  private final AbstractCummulativeStatistic waitTime;
  protected volatile List<Task> executorTasks = Collections.emptyList();

  public AbstractExecutor(int batchSize, int schedulingPeriodMillis, CyclicBarrier barrier,
      SchedulerState state) {
    this.batchSize = batchSize;
    this.schedulingPeriod = schedulingPeriodMillis;
    this.barrier = barrier;
    this.state = state;
    this.backoff = new SchedulerBackoff(BACKOFF_MIN_MILLIS, schedulingPeriodMillis,
        BACKOFF_RETRIES);
    this.index = indexGenerator.getAndIncrement();
    this.updateTime = new CountStatistic(
        StatisticPath.get(state.statisticsFolder, String.format(
            "Update-Action-Executor-%d", index),
            EXECUTOR_STATISTIC_TIME),
        false);
    this.waitTime = new CountStatistic(StatisticPath.get(state.statisticsFolder, String.format(
        "Wait-Barrier-Executor-%d", index), EXECUTOR_STATISTIC_TIME),
        false);
    updateTime.enable();
    waitTime.enable();
    initTaskDependencies(state.variableFeaturesWithDependencies());
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
    if (!updateTasks()) {
      return;
    }
    long startTime = System.currentTimeMillis();
    while (!Thread.currentThread().isInterrupted()) {
      boolean didRun = runNextTask();
      final long remainingTime = schedulingPeriod - (System.currentTimeMillis() - startTime);
      adjustUtilization(didRun, remainingTime);
      if (remainingTime <= 0) {
        if (!updateTasks()) {
          break;
        }
        startTime = System.currentTimeMillis();
      }
    }
    updateTime.disable();
    waitTime.disable();
  }

  private void adjustUtilization(boolean didRun, long remainingTime) {
    if (remainingTime <= 0) {
      return;
    }
    if (!didRun) {
      backoff.backoff(remainingTime);
      return;
    }
    backoff.relax();
  }

  protected abstract boolean runNextTask();

  private boolean updateTasks() {
    try {
      markUpdated();
      long start = System.currentTimeMillis();
      barrier.await();
      waitTime.append(System.currentTimeMillis() - start);
      return true;
    } catch (InterruptedException | BrokenBarrierException e) {
      return false;
    }
  }

  /**
   * Mark the task with that has the given index in the <b>LOCAL task list</b> as executed. Do
   * NOT use {@link Task#getIndex()} in this function, except if it matches the local index
   * (which it usually does not).
   *
   * @param localIndex The index of the task in executorTasks
   */
  protected final void mark(int localIndex) {
    runTasks.add(localIndex);
  }

  private void markUpdated() {
    long startTime = System.currentTimeMillis();
    for (Task task : executorTasks) {
      // Refresh features, for example those who are recorded as moving averages
      task.refreshFeatures();
    }
    for (int taskIndex : runTasks) {
      Task task = executorTasks.get(taskIndex);
      task.updateFeatures(state.variableFeaturesNoDependencies(),
          state.taskFeatures[task.getIndex()]);
      state.updated[task.getIndex()].set(true);
      for (TaskDependency taskDependency : taskDependencies) {
        for (Task dependent : taskDependency.dependents(task)) {
          state.updated[dependent.getIndex()].set(true);
        }
      }
    }
    runTasks.clear();
    updateTime.append(System.currentTimeMillis() - startTime);
  }

  @Override
  public String toString() {
    return "EXECUTOR: " + executorTasks + "\n";
  }
}
