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

package scheduling;

import common.Active;
import java.util.Collection;

/**
 * Scheduler for streaming operators.
 *
 * @author palivosd
 */
public interface LiebreScheduler<T extends Runnable> extends Active {

  /**
   * Add {@link Runnable}s to be scheduled by this entity. If the Scheduler implementation supports
   * live reconfigurations (during execution), this function can be called even when the SPE is
   * running.
   *
   * @param tasks The tasks to be scheduled.
   * @implNote This function should work correctly if called multiple times.
   * @throws IllegalStateException if the scheduler does not support live reconfigurations and this
   *     function is called while the SPE is active.
   */
  void addTasks(Collection<T> tasks);

  /**
   * Remove {@link Runnable}s that were to be scheduled by this entity. If the Scheduler
   * implementation supports live reconfigurations (during execution), this function can be called
   * even when the SPE is running.
   *
   * @param tasks The tasks to be removed from the schedule.
   * @implNote This function should work correctly if called multiple times.
   * @throws IllegalStateException if the scheduler does not support live reconfigurations and this
   *     function is called while the SPE is active.
   */
  void removeTasks(Collection<T> tasks);

  /** Start and schedule the tasks according to the actual scheduler implementation. */
  void startTasks();

  /** Stop the running tasks. */
  void stopTasks();

  /**
   * Activate statistics for this scheduler instance
   *
   * @param folder The folder to save the statistics to
   */
  void activateStatistics(String folder);

}
