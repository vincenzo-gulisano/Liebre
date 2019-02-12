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

import java.util.concurrent.atomic.AtomicBoolean;

public final class SchedulerState {

  final AtomicBoolean[] updated;
  final double[][] taskFeatures;
  final PriorityFunction priorityFunction;
  final String statisticsFolder;

  public SchedulerState(int nTasks, PriorityFunction priorityFunction, boolean priorityCachning,
      String statisticsFolder) {
    updated = new AtomicBoolean[nTasks];
    taskFeatures = new double[nTasks][Feature.length()];
    for (int i = 0; i < updated.length; i++) {
      updated[i] = new AtomicBoolean(false);
    }
    this.priorityFunction = priorityCachning ? priorityFunction.enableCaching(nTasks) :
        priorityFunction;
    this.statisticsFolder = statisticsFolder;
  }

}
