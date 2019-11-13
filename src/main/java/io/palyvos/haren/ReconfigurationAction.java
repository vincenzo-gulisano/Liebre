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
import common.statistic.HistogramStatistic;
import common.statistic.MeterStatistic;
import common.util.StatisticPath;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The functional object that performs the sequential part of what is referred to as the "scheduling
 * task" of {@link HarenScheduler}. Retrieves some of the features and runs the {@link
 * io.palyvos.haren.function.InterThreadSchedulingFunction} that assigns {@link Task}s to {@link
 * AbstractExecutor}s.
 */
class ReconfigurationAction implements Runnable {

  static final String STATISTIC_CALLS = "priocalls";
  static final String STATISTIC_TIME = "priotime";
  static final String BARRIER_INFO = "barrier-info";

  private static final Logger LOG = LogManager.getLogger();
  private final List<Task> tasks;
  private final List<AbstractExecutor> executors;
  private final SchedulerState state;
  private final AbstractCummulativeStatistic totalCalls;
  private final AbstractCummulativeStatistic updateTime;
  private final AbstractCummulativeStatistic deploymentTime;
  private final AbstractCummulativeStatistic barrierEnterVariance;
  private final AbstractCummulativeStatistic barrierExitVariance;
  private boolean firstUpdate = true;

  public ReconfigurationAction(
      List<Task> inputTasks, List<AbstractExecutor> executors, SchedulerState state) {
    this.tasks = new ArrayList(inputTasks);
    Collections.sort(tasks, Comparator.comparingInt(Task::getIndex));
    this.executors = executors;
    this.state = state;
    this.state.init(tasks);

    // Statistics Initialization
    this.totalCalls =
        new MeterStatistic(
            StatisticPath.get(
                state.statisticsFolder, statisticName("Total-Calls"), STATISTIC_CALLS),
            false);
    this.updateTime =
        new MeterStatistic(
            StatisticPath.get(
                state.statisticsFolder, statisticName("Update-Features"), STATISTIC_TIME),
            false);
    this.deploymentTime =
        new MeterStatistic(
            StatisticPath.get(
                state.statisticsFolder, statisticName("Deploy-Tasks"), STATISTIC_TIME),
            false);
    this.barrierEnterVariance =
        new HistogramStatistic(
            StatisticPath.get(
                state.statisticsFolder, statisticName("Enter-Variance"), BARRIER_INFO),
            false);
    this.barrierExitVariance =
        new HistogramStatistic(
            StatisticPath.get(state.statisticsFolder, statisticName("Exit-Variance"), BARRIER_INFO),
            false);
  }

  static String statisticName(String action) {
    return String.format("%s-Priority-Update", action);
  }

  private void enableStatistics() {
    totalCalls.enable();
    updateTime.enable();
    deploymentTime.enable();
    barrierEnterVariance.enable();
    barrierExitVariance.enable();
  }

  @Override
  public void run() {
    Validate.isTrue(tasks.size() > 0, "No tasks given!");
    if (firstUpdate) {
      updateAllFeatures();
      firstUpdate = false;
    } else {
      updateFeaturesWithDependencies();
    }
    state.intraThreadSchedulingFunction().clearCache();
    List<List<Task>> assignments = deployTasks();
    assignTasks(assignments);
    state.updateRoundEndTime();
    totalCalls.append(1);
    barrierEnterVariance.append(state.barrierEnterVariance());
    barrierExitVariance.append(state.barrierExitVariance());
  }

  private void updateFeaturesWithDependencies() {
    long startTime = System.currentTimeMillis();
    for (Task task : tasks) {
      if (state.resetUpdated(task)) {
        task.updateFeatures(
            state.variableFeaturesWithDependencies(),
            state.taskFeatures[state.indexer().schedulerIndex(task)]);
      }
    }
    updateTime.append(System.currentTimeMillis() - startTime);
  }

  private void updateAllFeatures() {
    long startTime = System.currentTimeMillis();
    for (Task task : tasks) {
      task.refreshFeatures();
      task.updateFeatures(
          state.constantFeatures(), state.taskFeatures[state.indexer().schedulerIndex(task)]);
      task.updateFeatures(
          state.variableFeaturesNoDependencies(),
          state.taskFeatures[state.indexer().schedulerIndex(task)]);
      task.updateFeatures(
          state.variableFeaturesWithDependencies(),
          state.taskFeatures[state.indexer().schedulerIndex(task)]);
    }
    updateTime.append(System.currentTimeMillis() - startTime);
  }

  private List<List<Task>> deployTasks() {
    long startTime = System.currentTimeMillis();
    List<List<Task>> assignments =
        state.interThreadSchedulingFunction().getAssignment(executors.size());
    deploymentTime.append(System.currentTimeMillis() - startTime);
    return assignments;
  }

  private void assignTasks(List<List<Task>> assignments) {
    Validate.isTrue(assignments.size() <= executors.size(), "#assignments > #threads");
    for (int threadId = 0; threadId < executors.size(); threadId++) {
      // Give no work to executors with no assignment
      List<Task> assignment =
          threadId < assignments.size() ? assignments.get(threadId) : Collections.emptyList();
      executors.get(threadId).setTasks(assignment);
    }
  }

  void stop() {
    totalCalls.disable();
    updateTime.disable();
    deploymentTime.disable();
    barrierEnterVariance.disable();
    barrierExitVariance.disable();
  }
}
