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

package io.palyvos.haren.function;

import io.palyvos.haren.Task;

public interface SingleIntraThreadSchedulingFunction extends IntraThreadSchedulingFunction {

  /**
   * Get the value of this function.
   *
   * @param task The task to get the value for.
   * @param features The feature matrix of all tasks.
   * @return The value of the function for the given task and the current values of the features.
   */
  double apply(Task task, double[][] features);

  /**
   * Usually if a {@link Task} has a higher value of a
   * {@link SingleIntraThreadSchedulingFunction}, it means that it has a higher priority. If the
   * <b>reverse</b> is true for this specific function, this should return {@code true}, so that
   * {@link io.palyvos.haren.HarenScheduler} can sort the tasks correctly by their priority.
   *
   * @return {@code true} if lower values of priority imply higher priority
   */
  default boolean isReverseOrder() {
    return false;
  }

  default SingleIntraThreadSchedulingFunction reverse() {
    return new ReverseIntraThreadSchedulingFunction(this);
  }

  @Override
  SingleIntraThreadSchedulingFunction enableCaching(int nTasks);

}
