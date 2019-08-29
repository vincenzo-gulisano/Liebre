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

public interface VectorIntraThreadSchedulingFunction extends IntraThreadSchedulingFunction {

  /**
   * Get the value vector of this function.
   *
   * @param task The task to get the value for.
   * @param features The feature matrix of all tasks.
   * @param output The value vector of the function for the given task and the current values of
   *     the features.
   */
  void apply(Task task, double[][] features, double[] output);

  /**
   * Get the number of dimensions of this function (i.e., the size of the resulting vector).
   *
   * @return The number of dimensions.
   */
  int dimensions();

  @Override
  VectorIntraThreadSchedulingFunction enableCaching(int nTasks);

  /**
   * Usually if a {@link Task} has a higher value of a
   * {@link SingleIntraThreadSchedulingFunction}, it means that it has a higher priority. If the
   * <b>reverse</b> is true for a specific function, {@code true} should be returned when its
   * index is queried, so that
   * {@link io.palyvos.haren.HarenScheduler} can sort the tasks correctly by their priority.
   *
   * @param i The index of the function
   * @return {@code true} if lower values of priority imply higher priority
   */
  boolean isReverseOrder(int i);
}
