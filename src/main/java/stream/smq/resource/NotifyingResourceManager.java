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

package stream.smq.resource;

import common.component.Component;
import common.component.EventType;
import java.util.concurrent.atomic.AtomicInteger;

public class NotifyingResourceManager extends AbstractResourceManager {

  private final Component component;
  private final EventType eventType;
  private final AtomicInteger acquisitionCounter = new AtomicInteger(0);

  public NotifyingResourceManager(int size, Component component, EventType eventType) {
    super(size);
    this.component = component;
    this.eventType = eventType;
  }

  @Override
  protected void doAcquire() throws InterruptedException {
    if (acquisitionCounter.incrementAndGet() == size) {
      component.waitFor(eventType);
    }
  }

  @Override
  protected void doRelease() {
    acquisitionCounter.decrementAndGet();
    component.notifyFor(eventType);
  }
}
