package stream.smq.resource;

public enum MultiSemaphoreFactory implements ResourceManagerFactory {
  INSTANCE;


  @Override
  public ResourceManager newResourceManager(int size) {
    return new MultiSemaphore(size);
  }
}
