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
      int schedulingPeriodExecutions, CyclicBarrier barrier, SchedulerState state) {
    super(batchSize, schedulingPeriodMillis, schedulingPeriodExecutions, barrier, state);
  }

  protected long runNextTask() {
    boolean executedSource = false;
    long executionTimeNanos = 0;
    for (int localIndex = 0; localIndex < executorTasks.size(); localIndex++) {
      Task task = executorTasks.get(localIndex);
//      LOG.debug("Trying to execute {}", task);
      if (task.canRun()) {
        LOG.debug("Executing {}", task);
        long startTime = System.nanoTime();
        task.runFor(batchSize);
        executionTimeNanos += System.nanoTime() - startTime;
        mark(localIndex);
        // Prevent starvation: If one source runs, then everything will be traversed until
        // we run out of components (enabling executedSource)
        boolean taskWasSource =
            Feature.COMPONENT_TYPE.get(task, state.taskFeatures) == FeatureHelper.CTYPE_SOURCE;
        executedSource = executedSource || taskWasSource;
        if (!executedSource) {
          break;
        }
      }
    }
    return executionTimeNanos;
  }

}
