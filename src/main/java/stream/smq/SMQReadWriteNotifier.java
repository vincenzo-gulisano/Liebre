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

import common.component.EventType;
import common.tuple.Tuple;
import stream.Stream;

public class SMQReadWriteNotifier<R extends Tuple> implements SmartMQReader, SmartMQWriter {

  private final Stream<R> stream;

  public SMQReadWriteNotifier(Stream<R> stream) {
    this.stream = stream;
  }

  @Override
  public <T extends Tuple> void put(int queueIndex, T value) {
    stream.addTuple((R) value);
  }

  @Override
  public <T extends Tuple> T take(int queueIndex) throws InterruptedException {
    return (T) stream.getNextTuple();
  }

  @Override
  public void waitForWrite(int queueIndex) throws InterruptedException {
    stream.getDestination().waitFor(EventType.WRITE);
  }

  @Override
  public void notifyWriteHappened(int queueIndex) {
    stream.getDestination().notifyFor(EventType.WRITE);
  }

  @Override
  public void notifyReadHappened(int queueIndex) throws InterruptedException {
    stream.getSource().notifyFor(EventType.READ);
  }

  @Override
  public void waitForRead(int queueIndex) throws InterruptedException {
    stream.getSource().waitFor(EventType.READ);
  }
}
