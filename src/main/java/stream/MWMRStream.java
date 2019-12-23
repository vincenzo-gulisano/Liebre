package stream;

public interface MWMRStream<T> extends Stream<T> {

  void registerReader(int readerIndex);

  void registerWriter(int writerIndex);
}
