package io.palyvos.haren;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * {@link TaskIndexer} that maintains an association of {@link Task} indexes to scheduler indexes to
 * make efficient use of the scheduler data structures in case of reconfigurations.
 *
 * <p>Internally, it uses a {@link HashMap} to maintain the remappings of indexes. For performance
 * reasons, only tasks whose scheduling index is different than the actual index are maintained in
 * the internal map.
 */
public class ReorderingTaskIndexer implements TaskIndexer {

  private final Map<Integer, Integer> taskIndexes = new HashMap<>();
  private final Set<Integer> freeIndexes = new HashSet<>();
  private int indexedTasksNumber;

  /**
   * Construct.
   *
   * @param indexedTasksNumber The number of tasks that are will be scheduled when this indexer is constructed.
   */
  ReorderingTaskIndexer(int indexedTasksNumber) {
    this.indexedTasksNumber = indexedTasksNumber;
  }

  /**
   * Return the index of the task for the scheduler data structures. This will be actual index of
   * the task (if no reorderings have occurred) or the reordered index in case of reorderings.
   *
   * @param task The task to get the index for.
   * @return The index of the task for scheduling purposes.
   */
  @Override
  public int schedulerIndex(Task task) {
    if (taskIndexes.isEmpty()) {
      return task.getIndex();
    } else {
      Integer mappedIndex = taskIndexes.get(task.getIndex());
      return mappedIndex != null ? mappedIndex : task.getIndex();
    }
  }

  /**
   * Register multiple tasks at a time.
   *
   * @param tasks The tasks to be registered.
   */
  @Override
  public synchronized void registerTasks(Collection<Task> tasks) {
    int neededIndexes = tasks.size() - freeIndexes.size();
    for (int i = indexedTasksNumber; i < indexedTasksNumber + neededIndexes; i++) {
      freeIndexes.add(i);
    }
    for (Task task : tasks) {
      registerTask(task);
    }
  }

  private synchronized void registerTask(Task task) {
    indexedTasksNumber += 1;
    final boolean taskIndexIsFree = freeIndexes.remove(task.getIndex());
    if (taskIndexIsFree) {
      // No need for reordering, schedulerIndex = task.getIndex()
      return;
    } else if (freeIndexes.isEmpty()) {
      // No free indexes, create new free index and reorder
      taskIndexes.put(indexedTasksNumber - 1, task.getIndex());
    } else {
      // Use a free index for reordering
      Iterator<Integer> freeIndexIterator = freeIndexes.iterator();
      int reorderIndex = freeIndexIterator.next();
      taskIndexes.put(task.getIndex(), reorderIndex);
      freeIndexIterator.remove();
    }
  }

  @Override
  public synchronized void unregisterTasks(Collection<Task> tasks) {
    for (Task task : tasks) {
      unregisterTask(task);
    }
  }

  @Override
  public int indexedTasks() {
    return indexedTasksNumber;
  }

  private synchronized void unregisterTask(Task task) {
    // Set the "scheduler" index of the task and add it to the free indexes
    Integer reorderIndex = taskIndexes.remove(task.getIndex());
    int schedulerIndex = Optional.ofNullable(reorderIndex).orElse(task.getIndex());
    freeIndexes.add(schedulerIndex);
    indexedTasksNumber -= 1;
  }
}
