package io.palyvos.haren;

import java.util.Collection;

public class NoopTaskIndexer implements TaskIndexer {

  @Override
  public int schedulerIndex(Task task) {
    return task.getIndex();
  }

  @Override
  public int registerTasks(Collection<Task> tasks) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int unregisterTasks(Collection<Task> tasks) {
    throw new UnsupportedOperationException();
  }
}
