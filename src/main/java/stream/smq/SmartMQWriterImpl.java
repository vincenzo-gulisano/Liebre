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

package stream.smq;

import common.tuple.Tuple;
import common.util.backoff.Backoff;
import common.util.backoff.NoopBackoff;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.Validate;
import stream.Stream;
import stream.smq.resource.ResourceManager;
import stream.smq.resource.ResourceManagerFactory;

public final class SmartMQWriterImpl implements SmartMQWriter, SmartMQController {

  private static final int FREE = 0;
  private static final int LOCKED = 1;
  private final ResourceManagerFactory rmFactory;
  private List<Stream<? extends Tuple>> queues = new ArrayList<>();
  private List<Backoff> backoffs = new ArrayList<>();
  private volatile boolean enabled;
  private ResourceManager writeSemaphore;

  public SmartMQWriterImpl(ResourceManagerFactory rmFactory) {
    this.rmFactory = rmFactory;
  }

  @Override
  public int register(Stream<? extends Tuple> stream, Backoff backoff) {
    int index = queues.size();
    queues.add(stream);
    backoffs.add(backoff);
    return index;
  }

  @Override
  public int register(Stream<? extends Tuple> stream) {
    return register(stream, NoopBackoff.INSTANCE);
  }

  @Override
  public void enable() {
    Validate.validState(queues.size() > 0, "queues");
    this.queues = Collections.unmodifiableList(queues);
    this.backoffs = Collections.unmodifiableList(backoffs);
    this.writeSemaphore = rmFactory.newResourceManager(queues.size());
    this.enabled = true;
  }

  @Override
  public boolean isEnabled() {
    return enabled;
  }

  @Override
  public void disable() {
    this.enabled = false;
  }

  @Override
  public <T extends Tuple> void put(int queueIndex, T value) throws InterruptedException {
    if (value == null) {
      throw new IllegalArgumentException("value");
    }
    Stream<T> queue = (Stream<T>) queues.get(queueIndex);
    if (!queue.offer(value)) {
      waitWrite(queueIndex);
    }
  }

  @Override
  public void notifyRead(int queueIndex) {
    writeSemaphore.release(queueIndex);
    backoffs.get(queueIndex).relax();
  }

  @Override
  public void waitWrite(int queueIndex) throws InterruptedException {
    writeSemaphore.acquire(queueIndex);
    backoffs.get(queueIndex).backoff();
  }


}
