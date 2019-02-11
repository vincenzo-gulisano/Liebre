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
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PriorityUpdateAction implements Runnable {

  static final String STATISTIC_CALLS = "priocalls";
  static final String STATISTIC_TIME = "priotime";
  private static final Logger LOG = LogManager.getLogger();
  private final List<Task> tasks;
  private final QueryResolver queries;
  private final List<AbstractExecutor> executors;
  private final double[] priorities;
  private final SchedulerState state;
  private final Comparator<Task> comparator;
  private final AbstractCummulativeStatistic totalCalls;
  private final AbstractCummulativeStatistic updateTime;
  private final AbstractCummulativeStatistic priorityTime;
  private final AbstractCummulativeStatistic distributionTime;
  private final AbstractCummulativeStatistic sortTime;
  private final double[][] priorityCache;


  public PriorityUpdateAction(List<Task> inputTasks,
      List<AbstractExecutor> executors,
      SchedulerState state) {
    this.tasks = new ArrayList(inputTasks);
    this.executors = executors;
    this.state = state;
    this.priorities = new double[tasks.size()];
    this.queries = new QueryResolver(this.tasks);
    this.comparator = createComparator(state);
    this.priorityCache = state.priorityFunction.newCache(tasks.size());
    this.totalCalls = new CountStatistic(StatisticPath.get(state.statisticsFolder, statisticName(
        "total"), STATISTIC_CALLS), true);
    totalCalls.enable();
    this.updateTime = new CountStatistic(StatisticPath.get(state.statisticsFolder, statisticName(
        "updateFeatures"), STATISTIC_TIME), true);
    updateTime.enable();
    this.priorityTime = new CountStatistic(StatisticPath.get(state.statisticsFolder, statisticName(
        "calculatePriorities"), STATISTIC_TIME), true);
    priorityTime.enable();
    this.distributionTime = new CountStatistic(
        StatisticPath.get(state.statisticsFolder, statisticName(
            "distributeTasks"), STATISTIC_TIME), true);
    distributionTime.enable();
    this.sortTime = new CountStatistic(StatisticPath.get(state.statisticsFolder, statisticName(
        "sortPriorities"), STATISTIC_TIME), true);
    sortTime.enable();
  }

  static String statisticName(String action) {
    return String.format("Priority-Update-%s", action);
  }

  private Comparator<Task> createComparator(SchedulerState state) {
    Comparator<Task> baseComparator = Comparator.comparingDouble(t -> priorities[t.getIndex()]);
    // Default sorting order for comparators is lowest to highest
    // Default sorting order for priorities is highest to lowest
    // So when reverseOrder() is true, we do not reverse the comparator and vice-versa
    return state.priorityFunction.reverseOrder() ? baseComparator : baseComparator.reversed();
  }

  @Override
  public void run() {
    Validate.isTrue(tasks.size() > 0, "No tasks given!");
    updateFeatures();
    calculatePriorities();
    List<List<Task>> assignments = distributeTasks();
    sortAndAssignTasks(assignments);
    totalCalls.append(1);
  }

  private void updateFeatures() {
    long startTime = System.currentTimeMillis();
    // Update features
    for (Task task : tasks) {
      if (!state.updated[task.getIndex()].getAndSet(false)) {
        //TODO: Maybe selectively do this on specific features
        task.updateFeatures();
        double[] taskFeatures = task.getFeatures(state.priorityFunction.features());
        storeTaskFeatures(task, taskFeatures);
      }
    }
    updateTime.append(System.currentTimeMillis() - startTime);
  }

  private void storeTaskFeatures(Task task, double[] taskFeatures) {
    final double[] row = state.taskFeatures[task.getIndex()];
    Validate.isTrue(row.length == taskFeatures.length);
    for (Feature feature : state.priorityFunction.features()) {
      int featureIndex = feature.index();
      row[featureIndex] = taskFeatures[featureIndex];
    }
  }

  private void calculatePriorities() {
    long startTime = System.currentTimeMillis();
    for (Task task : tasks) {
      priorities[task.getIndex()] = state.priorityFunction.apply(task, state.taskFeatures, priorityCache);
    }
    clearCache();
    priorityTime.append(System.currentTimeMillis() - startTime);
  }
  private void clearCache() {
    for (int i = 0; i < priorityCache.length; i++) {
      for (int j = 0; j < priorityCache[i].length; j++) {
       priorityCache[i][j] = -1;
      }
    }
  }

  private List<List<Task>> distributeTasks() {
    long startTime = System.currentTimeMillis();
    // Choose assignment of tasks -> threads
    List<List<Task>> assignments = new ArrayList<>();
    for (int i = 0; i < executors.size(); i++) {
      assignments.add(new ArrayList<>());
    }
    int assignmentIndex = 0;
    for (List<Task> query : queries.getQueries()) {
      assignments.get(assignmentIndex % assignments.size()).addAll(query);
      assignmentIndex++;
    }
    distributionTime.append(System.currentTimeMillis() - startTime);
    return assignments;
  }

  private void sortAndAssignTasks(List<List<Task>> assignments) {
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < executors.size(); i++) {
      List<Task> assignment = assignments.get(i);
      assignment.sort(comparator);
//      LOG.debug("-----Thread {} assignment-----", i);
//      for (Task task : assignment) {
//        LOG.debug("[{}, {}]", task, Arrays.toString(state.taskFeatures[task.getIndex()]));
//      }
      executors.get(i).setTasks(assignment);
    }
    sortTime.append(System.currentTimeMillis() - startTime);
  }

  // Helper, to be removed
  private double featureValue(Task task, Feature feature) {
    return state.taskFeatures[task.getIndex()][feature.index()];
  }

  private void recordUpdateFinished(long updateStartTime) {
    totalCalls.append(1);
    updateTime.append(System.currentTimeMillis() - updateStartTime);
  }

}
