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

package scheduling.thread;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Deque;
import java.util.concurrent.TimeUnit;

public class ExplicitWorkerThread extends LiebreThread {

  private static final int BATCH_SIZE = 10;
  private final Deque<ExecutionEntry> schedule;
  private final long quantum;
  private final TimeUnit unit;

  public ExplicitWorkerThread(Collection<ExecutionEntry> schedule, long quantum,
      TimeUnit unit) {
    this.schedule = new ArrayDeque(schedule);
    this.quantum = quantum;
    this.unit = unit;
  }

  @Override
  protected void doRun() {
    ExecutionEntry task = schedule.removeFirst();
    task.run(quantum, unit);
    schedule.addLast(task);
  }

}
