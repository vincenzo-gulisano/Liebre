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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PriorityUpdateAction implements Runnable {

  static final String STATISTIC_CALLS = "priocalls";
  static final String STATISTIC_TIME = "priotime";
  private static final Logger LOG = LogManager.getLogger();
  private final List<Task> tasks;
  private final List<AbstractExecutor> executors;
  private final SchedulerState state;
  private final AbstractCummulativeStatistic totalCalls;
  private final AbstractCummulativeStatistic updateTime;
  private final AbstractCummulativeStatistic priorityTime;
  private final AbstractCummulativeStatistic deploymentTime;
  private boolean firstUpdate = true;

  public PriorityUpdateAction(List<Task> inputTasks, List<AbstractExecutor> executors,
      SchedulerState state) {
    this.tasks = new ArrayList(inputTasks);
    this.executors = executors;
    this.state = state;
    this.state.init(inputTasks);

    // Statistics Initialization
    this.totalCalls = new CountStatistic(StatisticPath.get(state.statisticsFolder, statisticName(
        "Total-Calls"), STATISTIC_CALLS), true);
    totalCalls.enable();
    this.updateTime = new CountStatistic(StatisticPath.get(state.statisticsFolder, statisticName(
        "Update-Features"), STATISTIC_TIME), false);
    updateTime.enable();
    this.priorityTime = new CountStatistic(StatisticPath.get(state.statisticsFolder, statisticName(
        "Calculate-Priorities"), STATISTIC_TIME), false);
    priorityTime.enable();
    this.deploymentTime = new CountStatistic(
        StatisticPath.get(state.statisticsFolder, statisticName(
            "Deploy-Tasks"), STATISTIC_TIME), false);
    deploymentTime.enable();

  }

  static String statisticName(String action) {
    return String.format("%s-Priority-Update", action);
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
    calculatePriorities();
    List<List<Task>> assignments = deployTasks();
    assignTasks(assignments);
    totalCalls.append(1);
  }

  private void updateFeaturesWithDependencies() {
    long startTime = System.currentTimeMillis();
    for (Task task : tasks) {
      if (state.updated[task.getIndex()].getAndSet(false)) {
        task.updateFeatures(state.variableFeaturesWithDependencies(),
            state.taskFeatures[task.getIndex()]);
      }
    }
    updateTime.append(System.currentTimeMillis() - startTime);
  }

  private void updateAllFeatures() {
    long startTime = System.currentTimeMillis();
    for (Task task : tasks) {
      task.refreshFeatures();
      task.updateFeatures(state.constantFeatures(), state.taskFeatures[task.getIndex()]);
      task.updateFeatures(state.variableFeaturesNoDependencies(),
          state.taskFeatures[task.getIndex()]);
      task.updateFeatures(state.variableFeaturesWithDependencies(),
          state.taskFeatures[task.getIndex()]);
    }
    updateTime.append(System.currentTimeMillis() - startTime);
  }

  private void calculatePriorities() {
    long startTime = System.currentTimeMillis();
    for (Task task : tasks) {
      state.priorityFunction().apply(task, state.taskFeatures, state.priorities[task.getIndex()]);
    }
    state.priorityFunction().clearCache();
    priorityTime.append(System.currentTimeMillis() - startTime);
  }

  private List<List<Task>> deployTasks() {
    long startTime = System.currentTimeMillis();
    List<List<Task>> assignments = state.deploymentFunction().getDeployment(executors.size());
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

  void disable() {
    totalCalls.disable();
    updateTime.disable();
    priorityTime.disable();
    deploymentTime.disable();
  }

}
