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
      reader.notifyWrite(readerIndex);
    } catch (InterruptedException e) {
      LOGGER.info("addTuple() interrupted");
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public T getNextTuple() {
    try {
      T value = reader.take(readerIndex);
      if (value != null) {
        writer.notifyRead(writerIndex);
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
      this.reader = new SMQReaderNoop<>(decorated);
      this.writer = new SMQWriterNoop<>(decorated);
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

  private static final class SMQWriterNoop<R extends Tuple> implements SmartMQWriter {

    private final Stream<R> decorated;

    public SMQWriterNoop(Stream<R> decorated) {
      this.decorated = decorated;
    }


    @Override
    public <T extends Tuple> void put(int queueIndex, T value) {
      decorated.addTuple((R) value);
    }

    @Override
    public void notifyRead(int queueIndex) {
    }

    @Override
    public void waitWrite(int queueIndex) {
    }

  }


  private static final class SMQReaderNoop<R extends Tuple> implements SmartMQReader {

    private final Stream<R> decorated;

    public SMQReaderNoop(Stream<R> decorated) {
      this.decorated = decorated;
    }

    @Override
    public <T extends Tuple> T take(int queueIndex) {
      return (T) decorated.getNextTuple();
    }

    @Override
    public void notifyWrite(int queueIndex) {
    }

    @Override
    public void waitRead(int queueIndex) {
    }
  }

}
