package io.palyvos.haren;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ReorderingTaskIndexer implements TaskIndexer {

  private final Map<Integer, Integer> taskIndexes = new HashMap<>();
  private final Set<Integer> freeIndexes = new HashSet<>();
  private int nTasks;

  ReorderingTaskIndexer(int nTasks) {
    this.nTasks = nTasks;
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
      return taskIndexes.get(task.getIndex());
    }
  }

  /**
   * Register multiple tasks at a time.
   *
   * @param tasks The tasks to be registered.
   * @return The new number of tasks.
   */
  @Override
  public int registerTasks(Task... tasks) {
    int neededIndexes = tasks.length - freeIndexes.size();
    for (int i = nTasks; i < nTasks + neededIndexes; i++) {
      freeIndexes.add(i);
    }
    for (Task task : tasks) {
      registerTask(task);
    }
    return nTasks;
  }

  private void registerTask(Task task) {
    nTasks += 1;
    final boolean taskIndexIsFree = freeIndexes.remove(task.getIndex());
    if (taskIndexIsFree) {
      // No need for reordering, schedulerIndex = task.getIndex()
      return;
    } else if (freeIndexes.isEmpty()) {
      // No free indexes, create new free index and reorder
      taskIndexes.put(nTasks - 1, task.getIndex());
    } else {
      // Use a free index for reordering
      Iterator<Integer> freeIndexIterator = freeIndexes.iterator();
      int reorderIndex = freeIndexIterator.next();
      taskIndexes.put(task.getIndex(), reorderIndex);
      freeIndexIterator.remove();
    }
  }

  @Override
  public int unregisterTasks(Task... tasks) {
    for (Task task : tasks) {
      unregisterTask(task);
    }
    return nTasks;
  }

  private void unregisterTask(Task task) {
    // Set the "scheduler" index of the task and add it to the free indexes
    Integer reorderIndex = taskIndexes.remove(task.getIndex());
    int schedulerIndex = Optional.of(reorderIndex).orElse(task.getIndex());
    freeIndexes.add(schedulerIndex);
    nTasks -= 1;
  }
}
