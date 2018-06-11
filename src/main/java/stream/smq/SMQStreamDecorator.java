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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import stream.Stream;
import stream.StreamDecorator;

public class SMQStreamDecorator<T extends Tuple> extends StreamDecorator<T> {

  private static final Logger LOGGER = LogManager.getLogger();

  private final int readerIndex;
  private final SmartMQReader reader;
  private final int writerIndex;
  private final SmartMQWriter writer;

  protected SMQStreamDecorator(Builder<T> builder) {
    super(builder.decorated);
    this.reader = builder.reader;
    this.readerIndex = builder.readerIndex;
    this.writer = builder.writer;
    this.writerIndex = builder.writerIndex;
  }

  @Override
  public void addTuple(T tuple) {
    try {
      writer.put(writerIndex, tuple);
      reader.notifyWriteHappened(readerIndex);
    } catch (InterruptedException e) {
      LOGGER.info("addTuple() interrupted");
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public T getNextTuple() {
    try {
      T value = reader.take(readerIndex);
      if (value != null && !super.isFull()) {
        writer.notifyReadHappened(writerIndex);
      }
      return value;
    } catch (InterruptedException e) {
      LOGGER.info("getNextTuple() interrupted");
      Thread.currentThread().interrupt();
      return null;
    }
  }

  @Override
  public boolean offer(T tuple) {
    addTuple(tuple);
    return true;
  }

  @Override
  public int size() {
    return super.size();
  }

  @Override
  public T poll() {
    return getNextTuple();
  }

  public static class Builder<T extends Tuple> {

    private final Stream<T> decorated;

    private int readerIndex;
    private SmartMQReader reader;
    private int writerIndex;
    private SmartMQWriter writer;

    public Builder(Stream<T> decorated) {
      this.decorated = decorated;
    }

    public Builder useNotifications(boolean notifications) {
      if (notifications) {
        final SMQReadWriteNotifier<T> notifier = new SMQReadWriteNotifier<>(decorated);
        this.reader = notifier;
        this.writer = notifier;
      }
      else {
        final SMQNoop<T> noopReaderWriter = new SMQNoop<>(decorated);
        this.reader = noopReaderWriter;
        this.writer = noopReaderWriter;
      }
      return this;
    }

    public Builder reader(SmartMQReaderImpl reader, int index) {
      this.reader = reader;
      this.readerIndex = index;
      return this;
    }

    public Builder writer(SmartMQWriter writer, int index) {
      this.writer = writer;
      this.writerIndex = index;
      return this;
    }

    public SMQStreamDecorator<T> build() {
      return new SMQStreamDecorator<>(this);
    }

  }

  private static final class SMQNoop<R extends Tuple> implements SmartMQWriter,
      SmartMQReader {

    private final Stream<R> decorated;

    public SMQNoop(Stream<R> decorated) {
      this.decorated = decorated;
    }

    @Override
    public <T extends Tuple> void put(int queueIndex, T value) {
      decorated.addTuple((R) value);
    }

    @Override
    public <T extends Tuple> T take(int queueIndex) throws InterruptedException {
      return (T) decorated.getNextTuple();
    }

    @Override
    public void notifyReadHappened(int queueIndex) {
    }

    @Override
    public void waitForRead(int queueIndex) {
    }


    @Override
    public void waitForWrite(int queueIndex) throws InterruptedException {
    }

    @Override
    public void notifyWriteHappened(int queueIndex) {
    }
  }

}
