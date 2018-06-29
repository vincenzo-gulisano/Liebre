/*
 * Copyright (C) 2017-2018
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

package scheduling.thread;

import common.component.Component;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scheduling.TaskPool;

public class PoolWorkerThread extends LiebreThread {

  private static final Logger LOGGER = LogManager.getLogger();
  private final TaskPool<Component> taskPool;
  private final long quantumNanos;
  protected boolean executed; //FIXME: Does this need to be volatile?

  public PoolWorkerThread(int index, TaskPool<Component> availableTasks, long quantum,
      TimeUnit unit) {
    super(index);
    this.taskPool = availableTasks;
    this.quantumNanos = unit.toNanos(quantum);
  }

  @Override
  public void doRun() {
    Component task = getTask();
    if (task == null) {
      LOGGER.warn("Was given a null task to execute. Ignoring...");
      return;
    }
    executeTask(task);
    putTask(task);
  }

  protected Component getTask() {
    return taskPool.getNext(getIndex());
  }

  protected void executeTask(Component task) {
    executed = false;
    task.onScheduled();
    final long runUntil = System.nanoTime() + quantumNanos;
    if (task.outputsNumber().isMultiple()) {
      // Router Operators
      runMultipleOutputTask(task, runUntil);
    } else {
      // Everything else
      runSingleOutputTask(task, runUntil);
    }
  }

  private final void runSingleOutputTask(Component task, long runUntil) {
    while (System.nanoTime() < runUntil && task.canRun()) {
      task.run();
      executed = true;
    }
  }

  private final void runMultipleOutputTask(Component task, long runUntil) {
    while (System.nanoTime() < runUntil && task.canRead()) {
      task.run();
      executed = true;
      if (!task.canWrite()) {
        break;
      }
    }
  }

  protected void putTask(Component task) {
    if (executed) {
      task.onRun();
    }
    taskPool.put(task, getIndex());
  }

}
