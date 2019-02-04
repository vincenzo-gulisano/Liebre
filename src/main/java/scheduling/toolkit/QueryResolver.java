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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryResolver {

  private static final Logger LOG = LogManager.getLogger();
  private final Map<Integer, List<ExecutableComponent>> queryToComponents = new HashMap<>();
  private final int[] componentToQuery;
  private int nQueries;

  public QueryResolver(List<ExecutableComponent> components) {
    componentToQuery = new int[components.size()];
    resolveQueries(components);
  }

  private void resolveQueries(List<ExecutableComponent> components) {
    for (ExecutableComponent component : components) {
      traverseComponentGraph(component);
    }
    LOG.info("{} queries found", nQueries);
    for (ExecutableComponent component : components) {
      LOG.info("{} -> {}", component, componentToQuery[component.getIndex()]);
    }
    for (int queryNumber : queryToComponents.keySet()) {
      LOG.info("Query #{} -> {}", queryNumber, queryToComponents.get(queryNumber));
    }
  }

  private void traverseComponentGraph(ExecutableComponent component) {
    if (componentToQuery[component.getIndex()] > 0) { // Visited
      return;
    }
    nQueries += 1;
    final Deque<ExecutableComponent> q = new ArrayDeque<>();
    q.addLast(component);
    queryToComponents.put(nQueries, new ArrayList<>());
    while (!q.isEmpty()) {
      ExecutableComponent current = q.removeFirst();
      final int currentIndex = current.getIndex();
      if (componentToQuery[currentIndex] == 0) { // Not visited
        // Update both representations
        queryToComponents.get(nQueries).add(current);
        componentToQuery[currentIndex] = nQueries;
        // Recursively for the rest of the graph
        current.getDownstream().forEach(c -> q.addLast(c));
        current.getUpstream().forEach(c -> q.addLast(c));
      }
    }
  }

}
