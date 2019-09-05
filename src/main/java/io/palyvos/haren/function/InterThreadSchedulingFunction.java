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

import io.palyvos.haren.Feature;
import io.palyvos.haren.HarenScheduler;
import io.palyvos.haren.Task;
import java.util.List;

/**
 * Abstraction of a function that assigns {@link Task}s to
 * {@link HarenScheduler}'s processing threads.
 */
public interface InterThreadSchedulingFunction {

  /**
   * Initialize the function with the {@link Task}s that will be assigned and their
   * {@link Feature} matrix
   * @param tasks The {@link Task}s that this function will assign.
   * @param features The {@link Feature} matrix of these tasks.
   */
  void init(List<Task> tasks, double[][] features);

  /**
   * Get the assignment of {@link Task}s to processing threads.
   * @param nThreads The number of available processing threads.
   * @return A {@link List}, each element of which is the {@link Task}s that are assigned to the
   * thread with that index.
   */
  List<List<Task>> getAssignment(int nThreads);

  /**
   * Get the {@link Feature}s which are used to compute this function's value.
   *
   * @return An array of the {@link Feature}s required by this function.
   */
  Feature[] requiredFeatures();

  /**
   * @return The name of this function.
   */
  String name();

}
