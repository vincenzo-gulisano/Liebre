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
package common.component;

import common.Active;
import common.Named;
import scheduling.priority.PriorityMetric;

/**
 * Base interface for all stream components such as Sources, Sinks and Operators.
 *
 * @author palivosd
 */
public interface Component extends Active, Runnable, Named {

  void onScheduled();

  void onRun();

  void setPriorityMetric(PriorityMetric metric);

  ConnectionsNumber inputsNumber();

  ConnectionsNumber outputsNumber();

  void wait(EventType type);

  void notify(EventType type);

  default boolean canRead() {
    return true;
  }

  default boolean canWrite() {
    return true;
  }

  default boolean canRun() {
    return canRead() && canWrite();
  }

}
