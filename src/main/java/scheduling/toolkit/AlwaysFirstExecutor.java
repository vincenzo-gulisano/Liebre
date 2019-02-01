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

import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

class AlwaysFirstExecutor implements Runnable {

  /**
   * Number of operator executions before the {@link PriorityUpdateAction} is evoked.
   */
  public static final int UPDATE_PERIOD_EXECUTIONS = 1000;
  private static final Logger LOG = LogManager.getLogger();
  private final int nRounds;
  private final CyclicBarrier barrier;
  private volatile List<ExecutableComponent> tasks;

  public AlwaysFirstExecutor(int nRounds, CyclicBarrier barrier) {
    this.nRounds = nRounds;
    this.barrier = barrier;
  }

  public void setTasks(List<ExecutableComponent> tasks) {
    this.tasks = tasks;
  }

  @Override
  public void run() {
    int ctr = 0;
    try {
      LOG.info("WAITING on initial barrier");
      barrier.await();
    } catch (InterruptedException | BrokenBarrierException e) {
      return;
    }
    LOG.info("PASSED on initial barrier");
    while (!Thread.currentThread().isInterrupted()) {
      for (ExecutableComponent task : tasks) {
        if (task.canRun()) {
          task.runFor(nRounds);
          break;
        }
      }
      ctr = (ctr + 1) % UPDATE_PERIOD_EXECUTIONS;
      if (ctr == 0) {
        try {
          LOG.info("WAITING on loop barrier");
          barrier.await();
        } catch (InterruptedException | BrokenBarrierException e) {
          return;
        }
        LOG.info("PASSED on loop barrier");
      }
    }
  }

  @Override
  public String toString() {
    return "EXECUTOR: " + tasks + "\n";
  }
}
