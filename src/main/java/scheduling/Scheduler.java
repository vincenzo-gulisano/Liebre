package scheduling;

import java.util.Collection;

import operator.Operator;

/**
 * Scheduler for streaming operators.
 * 
 * @author palivosd
 *
 */
public interface Scheduler {
	/**
	 * Set the {@link Runnable}s that are going to be scheduled by this entity.
	 * These should generally be Operators and not sources or sinks.
	 * 
	 * @param tasks
	 *            The operators to be scheduled
	 */
	void addTasks(Collection<? extends Operator<?, ?>> tasks);

	/**
	 * Start and schedule the tasks according to the actual scheduler
	 * implementation.
	 */
	void startTasks();

	/**
	 * Stop the running tasks.
	 */
	void stopTasks();
}
