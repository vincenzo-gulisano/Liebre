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

import java.util.concurrent.CyclicBarrier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class HighestPriorityExecutor extends AbstractExecutor {

  private static final Logger LOG = LogManager.getLogger();

  public HighestPriorityExecutor(int batchSize, int schedulingPeriodMillis,
      CyclicBarrier barrier, SchedulerState state) {
    super(batchSize, schedulingPeriodMillis, barrier, state);
  }

  protected boolean runNextTask() {
    for (int localIndex = 0; localIndex < executorTasks.size(); localIndex++) {
      Task task = executorTasks.get(localIndex);
//      LOG.debug("Trying to execute {}", task);
      if (task.canRun()) {
        final boolean didRun = task.runFor(batchSize);
        if (didRun) {
          LOG.debug("Executed {}", task);
          runAction(localIndex, task);
          return true;
        }
      }
    }
    return false;
  }

  private void runAction(int localIndex, Task task) {
    if (isSource(task)) {
      runSources(localIndex + 1);
    }
    mark(localIndex);
  }

  private void runSources(int startIndex) {
    for (int localIndex = startIndex; localIndex < executorTasks.size(); localIndex++) {
      Task task = executorTasks.get(localIndex);
      if (task.canRun() && isSource(task)) {
        LOG.debug("[Source Round] Trying to execute source {}", task);
        task.runFor(batchSize);
        mark(localIndex);
      }
    }
  }

  private boolean isSource(Task task) {
    return Feature.COMPONENT_TYPE.get(task, state.taskFeatures) == FeatureHelper.CTYPE_SOURCE;
  }

}
