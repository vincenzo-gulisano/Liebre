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

/**
 * Abstraction of a function that prioritizes {@link io.palyvos.haren.Task}s assigned to one of
 * {@link HarenScheduler}'s processing threads.
 */
public interface IntraThreadSchedulingFunction {

  /**
   * Get the {@link Feature}s which are used to compute this function's value.
   *
   * @return An array of the {@link Feature}s required by this function.
   */
  Feature[] requiredFeatures();

  /**
   * Enable caching for a scheduling round. Caching enables a function implementation to maintain
   * values that might be reused in the same scheduling period (if, for example, priorities of one
   * task depend on the priority of some other task).
   *
   * @param nTasks The (maximum) number of tasks.
   * @return {@code this} for chaining
   */
  IntraThreadSchedulingFunction enableCaching(int nTasks);

  /** Clear the cache (if caching is enabled). Called at the end of every scheduling period. */
  void clearCache();

  /**
   * Reset the state of the function, and adjust the state to support the (new) given number of
   * tasks.
   *
   * @param nTasks The (new) number of tasks to be supported by this function.
   */
  default void reset(int nTasks) {
    if (cachingEnabled()) {
      enableCaching(nTasks);
    }
  }

  /**
   * Check if caching is enabled.
   *
   * @return {@code true} if caching is enabled.
   */
  boolean cachingEnabled();

  /** @return The name of this function. */
  String name();
}
