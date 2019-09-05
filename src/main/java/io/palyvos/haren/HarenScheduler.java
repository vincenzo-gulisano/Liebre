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

import io.palyvos.haren.function.InterThreadSchedulingFunction;
import io.palyvos.haren.function.SingleIntraThreadSchedulingFunction;
import io.palyvos.haren.function.VectorIntraThreadSchedulingFunction;
import io.palyvos.haren.function.VectorIntraThreadSchedulingFunctionImpl;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The scheduler class, responsible for orchestrating the execution of streaming {@link Task}s.
 */
//FIXME: Remove statistics from final version
public class HarenScheduler implements Scheduler<Task> {

  private static final Logger LOG = LogManager.getLogger();
  private final int batchSize;
  private final int schedulingPeriod;
  private final int nThreads;
  private final List<Task> tasks = new ArrayList<>();
  private final List<Thread> threads = new ArrayList<>();
  private final String statisticsFolder;
  private final VectorIntraThreadSchedulingFunction intraThreadFunction;
  private final boolean priorityCaching;
  private final InterThreadSchedulingFunction interThreadFunction;
  private final int[] workerAffinity;
  private volatile ReconfigurationAction reconfigurationAction;


  /**
   * Construct.
   *
   * @param nThreads The number of worker threads that will be used by Haren.
   * @param intraThreadFunction The desired
   *     {@link io.palyvos.haren.function.IntraThreadSchedulingFunction}, responsible for
   *     prioritizing the tasks executed by each thread.
   * @param interThreadFunction The desired {@link InterThreadSchedulingFunction},
   *     responsible for assigning tasks to worker threads.
   * @param caching Enable or disable caching (if supported by the chosen scheduling
   *     functions).
   * @param batchSize The maximum number of invocations of a scheduled tasks. Controls the
   *     preemption granularity.
   * @param schedulingPeriod The duration between two invocations of the scheduler, in millisec.
   * @param statisticsFolder The path for storing scheduling statistics.
   * @param workerAffinity Available CPU cores for the scheduler. Will be assigned to workers in
   *     a round-robin fashion.
   */
  public HarenScheduler(
      int nThreads, VectorIntraThreadSchedulingFunction intraThreadFunction,
      InterThreadSchedulingFunction interThreadFunction,
      boolean caching, int batchSize, int schedulingPeriod, String statisticsFolder,
      BitSet workerAffinity) {
    this.nThreads = nThreads;
    this.intraThreadFunction = intraThreadFunction;
    this.interThreadFunction = interThreadFunction;
    this.priorityCaching = caching;
    this.batchSize = batchSize;
    this.schedulingPeriod = schedulingPeriod;
    this.statisticsFolder = statisticsFolder;
    this.workerAffinity = workerAffinity.stream().toArray();
    if (this.workerAffinity.length < nThreads) {
      LOG.warn("#CPUs assigned is less than #threads! Performance might suffer.");
    }
  }

  /**
   * Helper constructor which accepts a {@link SingleIntraThreadSchedulingFunction} for
   * convenience.
   *
   * @see #HarenScheduler(int, VectorIntraThreadSchedulingFunction, InterThreadSchedulingFunction,
   *     boolean, int, int, String, BitSet)
   */
  public HarenScheduler(int nThreads, SingleIntraThreadSchedulingFunction intraThreadFunction,
      InterThreadSchedulingFunction interThreadFunction,
      boolean priorityCaching, int batchSize,
      int schedulingPeriod,
      String statisticsFolder, BitSet workerAffinity) {
    this(nThreads, new VectorIntraThreadSchedulingFunctionImpl(intraThreadFunction),
        interThreadFunction,
        priorityCaching, batchSize,
        schedulingPeriod, statisticsFolder, workerAffinity);
  }

  @Override
  public void start() {
    Validate.isTrue(tasks.size() >= nThreads, "Tasks less than threads!");
    LOG.info("Starting Scheduler");
    LOG.info("Priority Function: {}", intraThreadFunction);
    LOG.info("Priority Caching: {}", priorityCaching);
    LOG.info("Deployment Function Function: {}", interThreadFunction);
    LOG.info("Worker threads: {}", nThreads);
    LOG.info("Scheduling Period: {} ms", schedulingPeriod);
    LOG.info("Batch Size: {}", batchSize);
    final SchedulerState state = new SchedulerState(tasks.size(), intraThreadFunction,
        interThreadFunction, priorityCaching, statisticsFolder, nThreads, schedulingPeriod);
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

  @Override
  public void addTasks(Collection<Task> tasks) {
    this.tasks.addAll(tasks);
  }

  @Override
  public void stop() {
    reconfigurationAction.stop();
    for (Thread thread : threads) {
      thread.interrupt();
      try {
        thread.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  private int getAffinity(int i) {
    return workerAffinity != null ? workerAffinity[i % workerAffinity.length] : -1;
  }

  public List<Task> tasks() {
    return tasks;
  }
}
