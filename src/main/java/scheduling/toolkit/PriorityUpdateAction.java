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
  private final List<ExecutableComponent> components;
  private final List<AlwaysFirstExecutor> executors;
  private final PriorityFunction function;

  public PriorityUpdateAction(List<ExecutableComponent> components,
      List<AlwaysFirstExecutor> executors, PriorityFunction function) {
    this.components = components;
    this.executors = executors;
    this.function = function;
  }

  @Override
  public void run() {
    Validate.isTrue(components.size() >= executors.size());
    components.sort(Comparator.comparingDouble(c -> function.apply(c.getFeatures())));
    LOG.debug("Sorted components");
    for (ExecutableComponent component : components) {
      LOG.debug("[{}, {}]", component, function.apply(component.getFeatures()));
    }
    List<List<ExecutableComponent>> assignment = new ArrayList<>();
    for (int i = 0; i < executors.size(); i++) {
      assignment.add(new ArrayList<>());
    }
    for (int i = 0; i < components.size(); i++) {
      assignment.get(i % assignment.size()).add(components.get(i));
    }
    for (int i = 0; i < executors.size(); i++) {
      executors.get(i).setTasks(assignment.get(i));
    }
    //
    // Debugging code - WILL (RE)MOVE!
    //
    long currentTime = System.currentTimeMillis();
    for (ExecutableComponent task : components) {
      double[] features = task.getFeatures();
      LOG.debug("{}: ({}, {}, {}, {})\n", task, features[Features.F_COST],
          features[Features.F_SELECTIVITY],
          Features.getHeadLatency(features, currentTime), features[Features.F_COMPONENT_TYPE]);
    }
  }
}
