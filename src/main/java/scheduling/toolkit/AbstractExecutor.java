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

import static scheduling.toolkit.PriorityUpdateAction.STATISTIC_TIME;

import common.statistic.AbstractCummulativeStatistic;
import common.statistic.CountStatistic;
import common.util.StatisticPath;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class AbstractExecutor implements Runnable {

  private static final AtomicInteger indexGenerator = new AtomicInteger();
  private static final Logger LOG = LogManager.getLogger();
  protected final int batchSize;
  private final int schedulingPeriodMillis;
  private final int schedulingPeriodExecutions;
  protected final CyclicBarrier barrier;
  protected final SchedulerState state;
  private final Set<Integer> runTasks = new HashSet<>();
  private final Set<TaskDependency> taskDependencies = new HashSet<>();
  private final AbstractCummulativeStatistic markTime;
  protected volatile List<Task> executorTasks;

  public AbstractExecutor(int batchSize, int schedulingPeriodMillis, int schedulingPeriodExecutions,
      CyclicBarrier barrier, SchedulerState state) {
    this.batchSize = batchSize;
    this.schedulingPeriodMillis = schedulingPeriodMillis;
    this.schedulingPeriodExecutions = schedulingPeriodExecutions;
    this.barrier = barrier;
    this.state = state;
    this.markTime = new CountStatistic(StatisticPath.get(state.statisticsFolder, String.format(
        "AbstractExecutor-%d-markTime", indexGenerator.getAndIncrement()), STATISTIC_TIME), true);
    markTime.enable();
    initTaskDependencies(state.priorityFunction.features());
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
    int executions = 0;
    long executionTimeMillis = 0;
    while (!Thread.currentThread().isInterrupted()) {
      long startTime = System.currentTimeMillis();
      runNextTask();
      executions = (executions + 1) % schedulingPeriodExecutions;
      executionTimeMillis += System.currentTimeMillis() - startTime;
      if (executions == 0 || executionTimeMillis > schedulingPeriodMillis) {
        executionTimeMillis = 0;
        executions = 0;
        if (!updateTasks()) {
          return;
        }
      }
    }
  }

  protected abstract void onUpdatedTasks();

  protected abstract void runNextTask();

  private boolean updateTasks() {
    try {
      markUpdated();
      barrier.await();
      onUpdatedTasks();
      return true;
    } catch (InterruptedException | BrokenBarrierException e) {
      return false;
    }
  }

  private void markUpdated() {
    long startTime = System.currentTimeMillis();
    for (int taskIndex : runTasks) {
      Task task = executorTasks.get(taskIndex);
      state.updated[task.getIndex()].set(true);
      for (TaskDependency taskDependency : taskDependencies) {
        for (Task dependent : taskDependency.dependents(task)) {
          state.updated[dependent.getIndex()].set(true);
        }
      }
    }
    runTasks.clear();
    markTime.append(System.currentTimeMillis() - startTime);
  }

  @Override
  public String toString() {
    return "EXECUTOR: " + executorTasks + "\n";
  }
}
