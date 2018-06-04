package stream.smq;

import common.tuple.Tuple;

public interface SmartMQReader {

  void notifyWrite(int queueIndex);

  <T extends Tuple> T take(int queueIndex) throws InterruptedException;

  void waitRead(int queueIndex) throws InterruptedException;
}
