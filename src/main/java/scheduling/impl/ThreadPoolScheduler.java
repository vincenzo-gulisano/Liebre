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

import common.Active;
import common.component.Component;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scheduling.Scheduler;
import scheduling.TaskPool;
import scheduling.thread.LiebreThread;
import scheduling.thread.PoolWorkerThread;
import scheduling.thread.SourceThread;
import source.Source;

public class ThreadPoolScheduler implements Scheduler {

  private static final Logger LOGGER = LogManager.getLogger();

  private final TaskPool<Component> taskPool;
  private final List<PoolWorkerThread> workers = new ArrayList<>();
  private final int maxThreads;
  private final long quantum;
  private final TimeUnit timeUnit;
  private final List<SourceThread> sourceThreads = new ArrayList<>();
  private final List<Source<?>> sources = new ArrayList<>();
  private int nTasks;
  private String statsFolder;
  private String executionId;
  private volatile boolean indepedentSources;
  private volatile int nThreads;

  public ThreadPoolScheduler(int maxThreads, long quantum, TimeUnit unit,
      TaskPool<Component> taskPool) {
    Validate.notNull(taskPool, "taskPool");
    Validate.isTrue(maxThreads > 0, "maxThreads cannot be negative");
    Validate.isTrue(quantum > 0, "negative quantum is not allowed");
    this.taskPool = taskPool;
    this.maxThreads = maxThreads;
    this.quantum = quantum;
    this.timeUnit = unit;
  }

  public ThreadPoolScheduler enableSourceThreads() {
    Validate.validState(!isEnabled());
    LOGGER.warn("NEVER call enableSourceThreads() after enable(). Bad things will happen!");
    this.indepedentSources = true;
    return this;
  }

  @Override
  public void addTasks(Collection<? extends Component> tasks) {
    for (Component task : tasks) {
      if (indepedentSources && task instanceof Source) {
        sources.add((Source<?>) task);
        taskPool.registerPassive(task);
      } else {
        taskPool.register(task);
        nTasks++;
      }
    }
  }

  @Override
  public void startTasks() {
    Validate.validState(isEnabled(), "Cannot start tasks when TaskPool is disabled");
    LOGGER.info("Starting {} worker threads", nThreads);
    int threadIndex = 0;
    for (threadIndex = 0; threadIndex < nThreads; threadIndex++) {
      PoolWorkerThread worker = statsFolder != null
          ? new PoolWorkerThreadStatistic(threadIndex, taskPool, quantum, timeUnit, statsFolder, "")
          : new PoolWorkerThread(threadIndex, taskPool, quantum, timeUnit);
      workers.add(worker);
      worker.enable();
      worker.start();
    }
    // Independent source threads
    LOGGER.info("Starting {} source threads", sources.size());
    for (Component task : sources) {
      SourceThread t = new SourceThread(threadIndex, task, quantum, timeUnit);
      sourceThreads.add(t);
      t.enable();
      t.start();
      threadIndex++;
    }
  }

  @Override
  public void stopTasks() {
    Validate.validState(!isEnabled());
    for (LiebreThread workerThread : workers) {
      try {
        workerThread.disable();
        workerThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }
    workers.clear();
    for (LiebreThread workerThread : sourceThreads) {
      try {
        workerThread.disable();
        workerThread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        Thread.currentThread().interrupt();
      }
    }
    sourceThreads.clear();
  }

  @Override
  public void activateStatistics(String folder) {
    this.statsFolder = folder;
    this.executionId = executionId;
  }

  @Override
  public void enable() {
    nThreads = Math.min(maxThreads, nTasks);
    taskPool.setThreadsNumber(nThreads);
    taskPool.enable();
    for (Active s : sources) {
      s.enable();
    }
    LOGGER.info("Thread Pool Scheduling Enabled");
  }

  @Override
  public boolean isEnabled() {
    return taskPool.isEnabled();
  }

  @Override
  public void disable() {
    taskPool.disable();
    for (Active s : sources) {
      s.disable();
    }
  }

  @Override
  public boolean usesNotifications() {
    return true;
  }
}
