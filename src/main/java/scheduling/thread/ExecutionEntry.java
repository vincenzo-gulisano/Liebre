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

public final class ExecutionEntry {

  private final Component component;
  private final double quantumPercentage;
  private double remainingPercentage;

  public ExecutionEntry(Component component, double quantumPercentage) {
    this.component = component;
    this.quantumPercentage = quantumPercentage;
    this.remainingPercentage = quantumPercentage;
  }

  public void run(long quantum, TimeUnit unit) {
    long duration = Math.round(unit.toNanos(quantum) * quantumPercentage);
    long remainingTime =
        component.outputsNumber().isMultiple() ? runMultipleOutputTask(component, duration)
            : runSingleOutputTask(component, duration);
    //TODO: Use remaining percentage later if it makes sense
    remainingPercentage = remainingTime == 0 ? quantumPercentage : remainingTime / (double) quantum;
  }

  private long runSingleOutputTask(Component task, long duration) {
    long start = System.nanoTime();
    long remainingTime = duration;
    while (remainingTime > 0 && task.canRun()) {
      task.run();
      remainingTime = (start + duration) - System.nanoTime();
    }
    return remainingTime;
  }

  private long runMultipleOutputTask(Component task, long duration) {
    long start = System.nanoTime();
    long remainingTime = duration;
    while (remainingTime > 0 && task.canRun()) {
      task.run();
      remainingTime = (start + duration) - System.nanoTime();
      if (!task.canWrite()) {
        break;
      }
    }
    return remainingTime;
  }

}
