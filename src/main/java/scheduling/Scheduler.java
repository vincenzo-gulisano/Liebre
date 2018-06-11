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

package scheduling;

import java.util.Collection;

import common.Active;
import common.component.Component;

/**
 * Scheduler for streaming operators.
 *
 * @author palivosd
 */
public interface Scheduler extends Active {

  /**
   * Set the {@link Runnable}s that are going to be scheduled by this entity. These should generally
   * be Operators and not sources or sinks.
   *
   * @param tasks The operators to be scheduled
   */
  void addTasks(Collection<? extends Component> tasks);

  /**
   * Start and schedule the tasks according to the actual scheduler implementation.
   */
  void startTasks();

  /**
   * Stop the running tasks.
   */
  void stopTasks();

  /**
   * Activate statistics for this scheduler instance
   *
   * @param folder The folder to save the statistics to
   * @param executionId The ID of the execution
   */
  void activateStatistics(String folder, String executionId);

  boolean usesNotifications();

}
