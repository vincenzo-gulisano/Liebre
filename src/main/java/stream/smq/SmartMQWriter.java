package stream.smq;

import common.tuple.Tuple;

public interface SmartMQWriter {

  <T extends Tuple> void put(int queueIndex, T value) throws InterruptedException;

  int bufferSize(int index);

  void notifyRead(int queueIndex) throws InterruptedException;

  void waitWrite(int queueIndex) throws InterruptedException;
}
