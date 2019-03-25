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
  private int localIndex;
  private boolean equalPhase;
  private boolean[] isNextEqual;

  public HighestPriorityExecutor(int batchSize, int schedulingPeriodMillis,
      CyclicBarrier barrier, SchedulerState state) {
    super(batchSize, schedulingPeriodMillis, barrier, state);
  }

  protected boolean runNextTask() {
    boolean didRun = false;
    for (; localIndex < executorTasks.size(); localIndex++) {
      if (finishedEqualPhase()) {
        resetEqualPhase();
      }
      Task task = executorTasks.get(localIndex);
      if (task.canRun() && task.runFor(batchSize)) {
        LOG.trace("Executed {}", task);
//        if (equalPhase) {
//         LOG.info("Executed equal {}", task);
//        }
        mark(task, localIndex);
        didRun = true;
        break;
      }
    }
    // If a task ran and the next task has equal priority, continue execution from that one
    // This is called an "equal phase"
    if (didRun && isNextEqual[localIndex]) {
      enterEqualPhase();
    } else {
      // else start execution from highest priority
      resetEqualPhase();
    }
    return didRun;
  }

  @Override
  protected void onRoundStart() {
    markEqualPriorities();
    resetEqualPhase();
  }

  protected void markEqualPriorities() {
    isNextEqual = new boolean[executorTasks.size()];
    for (int i = 0; i < executorTasks.size() - 1; i++) {
      Task current = executorTasks.get(i);
      Task next = executorTasks.get(i + 1);
      if (state.comparator.compare(current, next) == 0) {
        isNextEqual[i] = true;
      }
    }
  }

  private void resetEqualPhase() {
    localIndex = 0;
    equalPhase = false;
  }

  private void enterEqualPhase() {
    equalPhase = true;
    localIndex = localIndex + 1;
  }

  private boolean finishedEqualPhase() {
    return equalPhase && !isNextEqual[localIndex - 1];
  }

}
