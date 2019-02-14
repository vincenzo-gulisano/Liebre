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

import component.Component;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scheduling.Scheduler;

public class ToolkitScheduler implements Scheduler<Task> {

  private static final Logger LOG = LogManager.getLogger();
  private final int batchSize;
  private final int schedulingPeriodMillis;
  private final int schedulingPeriodExecutions;
  private final int nThreads;
  private final List<Task> tasks = new ArrayList<>();
  private final List<Thread> threads = new ArrayList<>();
  private final String statisticsFolder;
  private final MultiPriorityFunction priorityFunction;
  private final boolean priorityCaching;

  public ToolkitScheduler(int nThreads, MultiPriorityFunction priorityFunction,
      boolean priorityCaching, int batchSize, int schedulingPeriodExecutions,
      int schedulingPeriodMillis,
      String statisticsFolder) {
    this.nThreads = nThreads;
    this.priorityFunction = priorityFunction;
    this.priorityCaching = priorityCaching;
    this.batchSize = batchSize;
    this.schedulingPeriodExecutions = schedulingPeriodExecutions;
    this.schedulingPeriodMillis = schedulingPeriodMillis;
    this.statisticsFolder = statisticsFolder;
  }

  public ToolkitScheduler(int nThreads, SinglePriorityFunction priorityFunction,
      boolean priorityCaching, int batchSize, int schedulingPeriodExecutions,
      int schedulingPeriodMillis,
      String statisticsFolder) {
    this(nThreads, new CombinedPriorityFunction(
            new AbstractPriorityFunction("min selecitivy", Feature.SELECTIVITY) {
              @Override
              public double apply(Task task, double[][] features) {
                if (Feature.SELECTIVITY.get(task, features) > 0.8) {
                  return 0;
                }
                return 1;
              }
            }, priorityFunction),
        priorityCaching, batchSize,
        schedulingPeriodExecutions, schedulingPeriodMillis, statisticsFolder);
  }


  @Override
  public void addTasks(Collection<Task> tasks) {
    this.tasks.addAll(tasks);
  }

  @Override
  public void startTasks() {
    Validate.isTrue(tasks.size() >= nThreads, "Tasks less than threads!");
    final SchedulerState state = new SchedulerState(tasks.size(), priorityFunction, priorityCaching,
        statisticsFolder);
    final List<AbstractExecutor> executors = new ArrayList<>();
    CyclicBarrier barrier = new CyclicBarrier(nThreads, new PriorityUpdateAction(tasks, executors,
        state));
    for (int i = 0; i < nThreads; i++) {
      executors.add(new HighestPriorityExecutor(batchSize, schedulingPeriodMillis,
          schedulingPeriodExecutions, barrier, state));
    }
    LOG.info("Using {} threads", executors.size());
    for (int i = 0; i < executors.size(); i++) {
      Thread t = new Thread(executors.get(i));
      t.setName(String.format("Scheduler-Worker-%d", i));
      threads.add(t);
      t.start();
    }
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
  }


}
