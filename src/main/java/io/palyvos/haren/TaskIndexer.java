package io.palyvos.haren;

import java.util.Arrays;
import java.util.Collection;

public interface TaskIndexer {

  int schedulerIndex(Task task);

  int registerTasks(Collection<Task> tasks);

  default int registerTasks(Task... tasks) {
    return registerTasks(Arrays.asList(tasks));
  }

  int unregisterTasks(Collection<Task> tasks);

  default int unregisterTasks(Task... tasks) {
    return unregisterTasks(Arrays.asList(tasks));
  }
}
