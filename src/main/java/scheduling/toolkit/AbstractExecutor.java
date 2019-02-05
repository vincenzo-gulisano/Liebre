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

public abstract class AbstractExecutor implements Runnable {

  /**
   * Number of operator executions before the {@link PriorityUpdateAction} is evoked.
   */
  public static final int UPDATE_PERIOD_EXECUTIONS = 1000;
  private static final Logger LOG = LogManager.getLogger();
  protected final int nRounds;
  protected final CyclicBarrier barrier;
  protected volatile List<Task> tasks;
  private int executions;

  public AbstractExecutor(
      int nRounds, CyclicBarrier barrier) {
    this.nRounds = nRounds;
    this.barrier = barrier;
  }

  public void setTasks(List<Task> tasks) {
    this.tasks = tasks;
  }

  @Override
  public void run() {
    if (!updateTasks()) {
      return;
    }
    while (!Thread.currentThread().isInterrupted()) {
      runNextTask();
      executions = (executions + 1) % UPDATE_PERIOD_EXECUTIONS;
      if (executions == 0) {
        if (!updateTasks()) {
          return;
        }
      }
    }
  }

  protected abstract void onUpdatedTasks();

  protected abstract void runNextTask();

  private boolean updateTasks() {
    try {
      LOG.debug("WAITING on barrier");
      barrier.await();
      LOG.debug("PASSED barrier");
      onUpdatedTasks();
      return true;
    } catch (InterruptedException | BrokenBarrierException e) {
      LOG.debug("Barrier INTERRUPTED!");
      return false;
    }
  }

  @Override
  public String toString() {
    return "EXECUTOR: " + tasks + "\n";
  }
}
