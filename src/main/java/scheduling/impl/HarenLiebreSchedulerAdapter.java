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

package scheduling.impl;

import component.Component;
import io.palyvos.haren.HarenScheduler;
import io.palyvos.haren.Task;
import java.util.Collection;
import scheduling.LiebreScheduler;

/**
 * Adapter for {@link HarenScheduler} to {@link LiebreScheduler}.
 */
public class HarenLiebreSchedulerAdapter implements LiebreScheduler<Task> {

  private final HarenScheduler scheduler;

  public HarenLiebreSchedulerAdapter(HarenScheduler scheduler) {
    this.scheduler = scheduler;
  }

  @Override
  public void addTasks(Collection<Task> tasks) {
    scheduler.addTasks(tasks);
  }

  @Override
  public void startTasks() {
    scheduler.start();
  }

  @Override
  public void stopTasks() {
    scheduler.stop();
  }

  @Override
  public void activateStatistics(String folder) {

  }

  public void setBatchSize(int batchSize) {
    scheduler.setBatchSize(batchSize);
  }

  public void setSchedulingPeriod(long schedulingPeriod) {
    scheduler.setSchedulingPeriod(schedulingPeriod);
  }

  @Override
  public void enable() {
    //FIXME: Liebre specific code, not needed for final toolkit
    for (Task task : scheduler.tasks()) {
      ((Component) task).enable();
    }
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public void disable() {
    //FIXME: Liebre specific code, not needed for final toolkit
    for (Task task : scheduler.tasks()) {
      ((Component) task).disable();
    }
  }
}
