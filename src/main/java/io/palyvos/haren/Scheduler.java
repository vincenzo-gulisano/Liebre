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

import java.util.Collection;

/** The entity that is responsible for scheduling {@link io.palyvos.haren.Task}s in an SPE. */
public interface Scheduler {

  /**
   * Add tasks to be scheduled by the {@link Scheduler} instance. Each {@link Task} can only be
   * added once. The function can be called multiple times, to add different sets of tasks. If the
   * scheduler implementation supports live reconfigurations, this function can also be called when
   * the scheduler is running.
   *
   * @param tasks The tasks to be scheduled.
   * @throws IllegalStateException if the scheduler implementation does not support live
   *     reconfigurations and this function is called while the scheduler is running
   */
  void addTasks(Collection<Task> tasks);

  /**
   * Remove tasks that were to be scheduled by the {@link Scheduler} instance.This function can be
   * called multiple times, to remove different sets of tasks. If the scheduler implementation
   * supports live reconfigurations, this function can also be called when the scheduler is running.
   *
   * @param tasks The tasks to be removed.
   * @throws IllegalStateException if the scheduler implementation does not support live *
   *     reconfigurations and this function is called while the scheduler is running
   */
  void removeTasks(Collection<Task> tasks);

  /** Start scheduling. */
  void start();

  /** Stop scheduling. */
  void stop();
}
