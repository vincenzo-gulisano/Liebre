package io.palyvos.haren;

import java.util.Collection;

public interface TaskIndexer {

  int schedulerIndex(Task task);

  void registerTasks(Collection<Task> tasks);

  void unregisterTasks(Collection<Task> tasks);

  int indexedTasks();
}
