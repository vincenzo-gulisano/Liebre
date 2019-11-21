package io.palyvos.haren;

import java.util.Collection;

/**
 * Entity responsible for assigning the correct indexes to the {@link Task}s scheduled by a {@link
 * Scheduler}, especially in case of live reconfigurations.
 */
public interface TaskIndexer {

  /**
   * Get the internal index for the given task, used for all scheduling purposes.
   *
   * @param task The {@link Task} to get the index for.
   * @return The scheduler index of the task.
   */
  int schedulerIndex(Task task);

  /**
   * Register new tasks to be handled by the indexer. Depending on the {@link TaskIndexer}
   * implementation, this might only need to be done for tasks added during a live reconfiguration.
   *
   * @param tasks The tasks to be registered in this {@link TaskIndexer}.
   */
  void registerTasks(Collection<Task> tasks);

  /**
   * Unregister existing tasks to will no longer be handled by the indexer. Depending on the {@link TaskIndexer}
   * implementation, this might only need to be done for tasks removed during a live reconfiguration.
   *
   * @param tasks The tasks to be unregistered in this {@link TaskIndexer}.
   */
  void unregisterTasks(Collection<Task> tasks);

  /**
   * @return The total number of tasks handled by this indexer.
   */
  int indexedTasks();
}
