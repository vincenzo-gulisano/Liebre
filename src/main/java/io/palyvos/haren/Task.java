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

import java.util.List;

/**
 * A streaming task that can be scheduled, e.g., an operator, a source or a sink.
 */
public interface Task extends Runnable {

  /**
   * Run this task for a maximum number of times, allowing preemption afterwards.
   *
   * @param times The maximum number of times that this task will be executed.
   * @return {@code true} if this task actually executed at least once. Might return false, for
   *     example, if there was nothing to process and the task immediately exited.
   */
  boolean runFor(final int times);

  /**
   * Check if there is any work that can be executed by this task.
   *
   * @return {@code true} if this task can be executed and perform meaningful work.
   */
  boolean canRun();

  /**
   * Update the features of this task
   *
   * @param features The specific features to be updated.
   * @param output The array where feature values will be written (indexes will be decided based
   *     on Feature{@link #getIndex()})
   */
  void updateFeatures(Feature[] features, double[] output);

  /**
   * Do an internal refresh of execution-dependent features. If an operator maintains an running
   * average of its throughput, a call to this function might trigger an update of that average.
   */
  void refreshFeatures();

  /**
   * @return The unique numerical index of this task.
   */
  int getIndex();

  /**
   * Get all the upstream tasks in the query graph.
   *
   * @return A list of the upstream tasks.
   */
  List<? extends Task> getUpstream();

  /**
   * Get all the downstream tasks in the query graph.
   *
   * @return A list of the downstream tasks.
   */
  List<? extends Task> getDownstream();

}
