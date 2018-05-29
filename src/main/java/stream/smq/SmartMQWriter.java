package stream.smq;

import common.tuple.Tuple;

public interface SmartMQWriter {

  <T extends Tuple> void offer(int queueIndex, T value);

  void notifyRead(int queueIndex);

  void waitWrite(int queueIndex);
}
