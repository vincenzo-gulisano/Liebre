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

package io.palyvos.haren;

import component.Component;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scheduling.Scheduler;

public class HarenScheduler implements Scheduler<Task> {

  private static final Logger LOG = LogManager.getLogger();
  private final int batchSize;
  private final int schedulingPeriod;
  private final int nThreads;
  private final List<Task> tasks = new ArrayList<>();
  private final List<Thread> threads = new ArrayList<>();
  private final String statisticsFolder;
  private final MultiIntraThreadSchedulingFunction priorityFunction;
  private final boolean priorityCaching;
  private final InterThreadSchedulingFunction interThreadSchedulingFunction;
  private final int[] workerAffinity;
  private volatile ReconfigurationAction reconfigurationAction;


  public HarenScheduler(
      int nThreads, MultiIntraThreadSchedulingFunction priorityFunction, InterThreadSchedulingFunction interThreadSchedulingFunction,
      boolean priorityCaching, int batchSize, int schedulingPeriod, String statisticsFolder,
      BitSet workerAffinity) {
    this.nThreads = nThreads;
    this.priorityFunction = priorityFunction;
    this.interThreadSchedulingFunction = interThreadSchedulingFunction;
    this.priorityCaching = priorityCaching;
    this.batchSize = batchSize;
    this.schedulingPeriod = schedulingPeriod;
    this.statisticsFolder = statisticsFolder;
    this.workerAffinity = workerAffinity.stream().toArray();
    if (this.workerAffinity.length < nThreads) {
      LOG.warn("#CPUs assigned is less than #threads! Performance might suffer.");
    }
  }

  public HarenScheduler(int nThreads, SingleIntraThreadSchedulingFunction priorityFunction,
      InterThreadSchedulingFunction interThreadSchedulingFunction,
      boolean priorityCaching, int batchSize,
      int schedulingPeriod,
      String statisticsFolder, BitSet workerAffinity) {
    this(nThreads, new CombinedIntraThreadSchedulingFunction(priorityFunction),
        interThreadSchedulingFunction,
        priorityCaching, batchSize,
        schedulingPeriod, statisticsFolder, workerAffinity);
  }


  @Override
  public void addTasks(Collection<Task> tasks) {
    this.tasks.addAll(tasks);
  }

  @Override
  public void startTasks() {
    Validate.isTrue(tasks.size() >= nThreads, "Tasks less than threads!");
    LOG.info("Starting Scheduler");
    LOG.info("Priority Function: {}", priorityFunction);
    LOG.info("Priority Caching: {}", priorityCaching);
    LOG.info("Deployment Function Function: {}", interThreadSchedulingFunction);
    LOG.info("Worker threads: {}", nThreads);
    LOG.info("Scheduling Period: {} ms", schedulingPeriod);
    LOG.info("Batch Size: {}", batchSize);
    final SchedulerState state = new SchedulerState(tasks.size(), priorityFunction,
        interThreadSchedulingFunction, priorityCaching, statisticsFolder, nThreads, schedulingPeriod);
    final List<AbstractExecutor> executors = new ArrayList<>();
    this.reconfigurationAction = new ReconfigurationAction(tasks, executors, state);
    CyclicBarrier barrier = new CyclicBarrier(nThreads, reconfigurationAction);
    for (int i = 0; i < nThreads; i++) {
      int cpuId = getAffinity(i);
      executors.add(new HighestPriorityExecutor(batchSize, schedulingPeriod, barrier, state,
          cpuId));
    }
    for (int i = 0; i < executors.size(); i++) {
      Thread t = new Thread(executors.get(i));
      t.setName(String.format("Scheduler-Worker-%d", i));
      threads.add(t);
      t.start();
    }
  }

  private int getAffinity(int i) {
    return workerAffinity != null ? workerAffinity[i % workerAffinity.length] : -1;
  }

  @Override
  public void stopTasks() {
    for (Thread thread : threads) {
      thread.interrupt();
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void activateStatistics(String folder) {

  }

  @Override
  public void enable() {
    //FIXME: Liebre specific code, not needed for final toolkit
    for (Task task : tasks) {
      ((Component) task).enable();
    }
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public void disable() {
    //FIXME: Liebre specific code, not needed for final toolkit
    for (Task task : tasks) {
      ((Component) task).disable();
    }
    reconfigurationAction.disable();
  }


}
