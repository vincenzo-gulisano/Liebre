package io.palyvos.haren;

public interface TaskIndexer {

  int schedulerIndex(Task task);

  int registerTasks(Task... tasks);

  int unregisterTasks(Task... tasks);
}
