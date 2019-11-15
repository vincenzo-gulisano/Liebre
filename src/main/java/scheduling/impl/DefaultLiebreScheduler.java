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

package scheduling.impl;

import component.Component;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scheduling.LiebreScheduler;
import scheduling.thread.BasicWorkerThread;

/**
 * Scheduler implementation in case no scheduling is actually needed and the requirement is just one
 * thread per component.operator.
 *
 * @author palivosd
 */
public class DefaultLiebreScheduler implements LiebreScheduler<Component> {

  private static final Logger LOGGER = LogManager.getLogger();

  private final List<Component> tasks = new ArrayList<>();
  private final List<BasicWorkerThread> threads = new ArrayList<>();
  private final BitSet affinity;
  private volatile boolean enabled;

  public DefaultLiebreScheduler() {
    this(null);
  }

  public DefaultLiebreScheduler(BitSet affinity) {
    this.affinity = affinity;
  }

  @Override
  public void addTasks(Collection<Component> tasks) {
    if (isEnabled()) {
      throw new IllegalStateException();
    }
    this.tasks.addAll(tasks);
  }

  @Override
  public void removeTasks(Collection<Component> tasks) {
    if (isEnabled()) {
      throw new IllegalStateException();
    }
    this.tasks.removeAll(tasks);
  }

  @Override
  public void startTasks() {
    if (!isEnabled()) {
      throw new IllegalStateException();
    }
    for (Runnable task : tasks) {
      BasicWorkerThread thread = new BasicWorkerThread(task, affinity);
      thread.setName(task.toString());
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
    LOGGER.info("Basic Scheduling Enabled");
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

}

