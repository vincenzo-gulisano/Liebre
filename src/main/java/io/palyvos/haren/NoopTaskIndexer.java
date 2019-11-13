package io.palyvos.haren;

public class NoopTaskIndexer implements TaskIndexer {

  @Override
  public int schedulerIndex(Task task) {
    return task.getIndex();
  }

  @Override
  public int registerTasks(Task... tasks) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int unregisterTasks(Task... tasks) {
    throw new UnsupportedOperationException();
  }
}
