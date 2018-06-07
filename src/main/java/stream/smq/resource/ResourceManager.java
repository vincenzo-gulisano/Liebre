package stream.smq.resource;

public interface ResourceManager {

  void acquire(int index) throws InterruptedException;

  void release(int index);
}
