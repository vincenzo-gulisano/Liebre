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

package scheduling.impl;

import common.component.Component;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scheduling.Scheduler;
import scheduling.thread.BasicWorkerThread;

/**
 * Scheduler implementation in case no scheduling is actually needed and the requirement is just one
 * thread per operator.
 *
 * @author palivosd
 */
public class NoopScheduler implements Scheduler {

  private static final Logger LOGGER = LogManager.getLogger();

  private final List<Component> tasks = new ArrayList<>();
  private final List<BasicWorkerThread> threads = new ArrayList<>();
  private volatile boolean enabled;

  @Override
  public void addTasks(Collection<? extends Component> tasks) {
    this.tasks.addAll(tasks);
  }

  @Override
  public void startTasks() {
    if (!isEnabled()) {
      throw new IllegalStateException();
    }
    for (Runnable operator : tasks) {
      BasicWorkerThread thread = new BasicWorkerThread(operator);
      threads.add(thread);
      thread.enable();
      thread.start();
    }
  }

  @Override
  public void stopTasks() {
    if (isEnabled()) {
      throw new IllegalStateException();
    }
    for (BasicWorkerThread thread : threads) {
      try {
        thread.disable();
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void enable() {
    for (Component task : tasks) {
      task.enable();
    }
    this.enabled = true;
  }

  @Override
  public boolean isEnabled() {
    return this.enabled;
  }

  @Override
  public void disable() {
    this.enabled = false;
    for (Component task : tasks) {
      task.disable();
    }
  }

  @Override
  public void activateStatistics(String folder) {
    LOGGER.info("No statistics available");
  }

  @Override
  public boolean usesNotifications() {
    return false;
  }
}
