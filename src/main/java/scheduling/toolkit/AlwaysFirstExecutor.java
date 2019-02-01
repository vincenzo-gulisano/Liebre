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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class AlwaysFirstExecutor implements Runnable {

  private final List<ExecutableComponent> tasks = new ArrayList<>();
  private final int nRounds;

  public AlwaysFirstExecutor(int nRounds) {
    this(Collections.emptyList(), nRounds);
  }

  public AlwaysFirstExecutor(List<ExecutableComponent> tasks, int nRounds) {
    this.tasks.addAll(tasks);
    this.nRounds = nRounds;
  }

  public void addTask(ExecutableComponent task) {
    tasks.add(task);
  }

  @Override
  public void run() {
    int ctr = 0;
    while (!Thread.currentThread().isInterrupted()) {
      for (ExecutableComponent task : tasks) {
        if (task.canRun()) {
          task.runFor(nRounds);
          break;
        }
      }
      ctr = (ctr + 1) % 1000;
      if (ctr == 0) {
        for (ExecutableComponent task : tasks) {
          double[] features = task.getFeatures();
          System.out.format("%s: (%4.3f, %8.7f)\n", task, features[Features.F_COST],
              features[Features.F_SELECTIVITY]);
        }
      }
    }
  }

  @Override
  public String toString() {
    return "EXECUTOR: " + tasks.toString() +  "\n";
  }
}
