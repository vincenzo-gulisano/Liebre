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
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import org.apache.commons.lang3.Validate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import scheduling.Scheduler;

public class ToolkitScheduler implements Scheduler {

  private static final Logger LOG = LogManager.getLogger();
  private final int nRounds;
  private final int nThreads;
  private final List<ExecutableComponent> tasks = new ArrayList<>();
  private final List<Thread> threads = new ArrayList<>();

  public ToolkitScheduler(int nRounds, int nThreads) {
    this.nRounds = nRounds;
    this.nThreads = nThreads;
  }

  @Override
  public void addTasks(Collection<? extends Component> tasks) {
    this.tasks.addAll(tasks);
  }

  @Override
  public void startTasks() {
    tasks.sort(Comparator.comparingDouble(c -> c.getFeatures()[0]));
    LOG.info("Sorted tasks: {}", tasks);
    Validate.isTrue(tasks.size() >= nThreads);
    final List<AlwaysFirstExecutor> executors = new ArrayList<>();
    CyclicBarrier barrier = new CyclicBarrier(nThreads, new PriorityUpdateAction(tasks, executors
        , features -> features[Features.F_TOPOLOGICAL_ORDER]));
    for (int i = 0; i < nThreads; i++) {
      executors.add(new AlwaysFirstExecutor(nRounds, barrier));
    }
    LOG.info("Executors\n {}", executors);
    LOG.info("Using {} threads", executors.size());
    for (Runnable executor : executors) {
      Thread t = new Thread(executor);
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
    for (ExecutableComponent task : tasks) {
      ((Component) task).enable();
    }
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public void disable() {
    for (ExecutableComponent task : tasks) {
      ((Component) task).disable();
    }
  }

  private static class RoundRobinExecutor implements Runnable {

    private final List<ExecutableComponent> tasks = new ArrayList<>();
    private final int nRounds;

    public RoundRobinExecutor(List<ExecutableComponent> tasks, int nRounds) {
      this.tasks.addAll(tasks);
      this.nRounds = nRounds;
    }

    @Override
    public void run() {
      int index = 0;
      final int size = tasks.size();
      while (!Thread.currentThread().isInterrupted()) {
        ExecutableComponent task = tasks.get(index++ % size);
        if (task.canRun()) {
//          LOG.info("Running {}", task);
          task.runFor(nRounds);
        }
      }
    }
  }
}
