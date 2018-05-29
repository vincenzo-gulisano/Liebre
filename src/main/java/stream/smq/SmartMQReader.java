package stream.smq;

import common.tuple.Tuple;

public interface SmartMQReader {

  void notifyWrite(int queueIndex);

  <T extends Tuple> T poll(int queueIndex);

  void waitRead(int queueIndex);
}
