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
import common.util.BinPacking;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scheduling.Scheduler;
import scheduling.thread.ExecutionEntry;
import scheduling.thread.LiebreThread;
import scheduling.thread.ExplicitWorkerThread;

public class ExplicitScheduler implements Scheduler {

  private static final Logger LOGGER = LogManager.getLogger();
  private final Map<String, Double> utilizations;
  private final List<ExecutionEntry> executionEntries = new ArrayList<>();
  private final List<LiebreThread> threads = new ArrayList<>();
  private volatile boolean enabled;
  private final int nThreads;
  private final long quantum;
  private final TimeUnit timeUnit;

  public ExplicitScheduler(Map<String, Double> utilizations, int nThreads, long quantum, TimeUnit timeUnit) {
    this.utilizations = utilizations;
    this.nThreads = nThreads;
    this.quantum = quantum;
    this.timeUnit = timeUnit;
  }


  @Override
  public void addTasks(Collection<? extends Component> tasks) {
    for (Component task : tasks) {
      Double percentage = utilizations.get(task.getId());
      Validate.notNull(percentage, "Missing utilization data for task %s", task);
      executionEntries.add(new ExecutionEntry(task, percentage));
    }
  }

  @Override
  public void startTasks() {
    LOGGER.info("Starting {} threads", threads.size());
    for (LiebreThread thread : threads) {
      thread.start();
    }
  }

  @Override
  public void stopTasks() {
    for (LiebreThread thread : threads) {
      try {
        thread.disable();
        thread.join();
      } catch (InterruptedException exception) {
        LOGGER.info("Interrupted", exception);
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public void activateStatistics(String folder) {
    LOGGER.info("No statistics available");
  }

  @Override
  public boolean usesNotifications() {
    return true;
  }

  @Override
  public void enable() {
    executionEntries.forEach(e -> e.enable());
    List<List<ExecutionEntry>> schedules = BinPacking.INSTANCE.split(executionEntries, nThreads);
    LOGGER.info("Explicit schedules calculated");
    for (int i = 0; i < schedules.size(); i++) {
      List<ExecutionEntry> schedule = schedules.get(i);
      LOGGER.info("Schedule for thread {} -> {}", i, schedule);
      final LiebreThread thread = new ExplicitWorkerThread(schedule, quantum, timeUnit);
      threads.add(thread);
      thread.enable();
    }
    this.enabled = true;
    LOGGER.info("Explicit scheduling enabled");
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void disable() {
    this.enabled = false;
    executionEntries.forEach(e -> e.disable());
  }
}
