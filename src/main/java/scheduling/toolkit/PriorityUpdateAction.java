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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PriorityUpdateAction implements Runnable {

  private static final Logger LOG = LogManager.getLogger();
  private final List<Task> tasks;
  private final QueryResolver queries;
  private final List<AbstractExecutor> executors;
  private final PriorityFunction function;
  private final Comparator<Task> comparator;

  public PriorityUpdateAction(List<Task> inputTasks,
      List<AbstractExecutor> executors, PriorityFunction function) {
    this.tasks = new ArrayList(inputTasks);
    this.queries = new QueryResolver(this.tasks);
    this.executors = executors;
    this.function = function;
    this.comparator =
        Comparator.<Task>comparingDouble(c -> function.apply(c)).reversed();
  }

  @Override
  public void run() {
    Validate.isTrue(tasks.size() >= executors.size());
    // Update features
    for (Task task : tasks) {
      task.updateFeatures();
    }
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
    for (int i = 0; i < executors.size(); i++) {
      List<Task> assignment = assignments.get(i);
      assignment.sort(comparator);
      LOG.debug("-----Thread {} assignment-----", i);
      for (Task task : assignment) {
        LOG.debug("[{}, {}]", task, function.apply(task));
      }
      executors.get(i).setTasks(assignment);
    }
    //
    // Debugging code - WILL (RE)MOVE!
    //
    long currentTime = System.currentTimeMillis();
    LOG.debug("--------ALL FEATURES--------");
    for (Task task : tasks) {
      double[] features = task.getFeatures();
      LOG.debug("{}: ({}, {}, {}, {}, {})", task, features[Features.F_COST],
          features[Features.F_SELECTIVITY],
          Features.getHeadLatency(features, currentTime),
          features[Features.F_AVERAGE_ARRIVAL_TIME], features[Features.F_COMPONENT_TYPE]
          );
    }
  }
}
