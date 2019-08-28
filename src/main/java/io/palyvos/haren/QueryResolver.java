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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryResolver {

  private static final Logger LOG = LogManager.getLogger();
  private final Map<Integer, List<Task>> queryToTasks = new HashMap<>();
  private final int[] taskToQuery;
  private int nQueries;

  public QueryResolver(List<Task> tasks) {
    taskToQuery = new int[tasks.size()];
    resolveQueries(tasks);
  }

  public Collection<List<Task>> getQueries() {
    return queryToTasks.values();
  }

  private void resolveQueries(List<Task> tasks) {
    for (Task task : tasks) {
      traverseTaskGraph(task);
    }
    LOG.info("{} queries found", nQueries);
    for (int queryNumber : queryToTasks.keySet()) {
      LOG.debug("Query #{} -> {}", queryNumber, queryToTasks.get(queryNumber));
    }
  }

  private void traverseTaskGraph(Task task) {
    if (taskToQuery[task.getIndex()] > 0) { // Visited
      return;
    }
    nQueries += 1;
    final Deque<Task> q = new ArrayDeque<>();
    q.addLast(task);
    queryToTasks.put(nQueries, new ArrayList<>());
    while (!q.isEmpty()) {
      Task current = q.removeFirst();
      final int currentIndex = current.getIndex();
      if (taskToQuery[currentIndex] == 0) { // Not visited
        // Update both representations
        queryToTasks.get(nQueries).add(current);
        taskToQuery[currentIndex] = nQueries;
        // Recursively for the rest of the graph
        current.getDownstream().forEach(t -> q.addLast(t));
        current.getUpstream().forEach(t -> q.addLast(t));
      }
    }
  }

}
