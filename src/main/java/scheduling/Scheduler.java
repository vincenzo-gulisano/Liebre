package scheduling;

import java.util.Collection;

import common.Active;
import common.component.Component;

/**
 * Scheduler for streaming operators.
 * 
 * @author palivosd
 *
 */
public interface Scheduler extends Active {
	/**
	 * Set the {@link Runnable}s that are going to be scheduled by this entity.
	 * These should generally be Operators and not sources or sinks.
	 * 
	 * @param tasks
	 *            The operators to be scheduled
	 */
	void addTasks(Collection<? extends Component> tasks);

	/**
	 * Start and schedule the tasks according to the actual scheduler
	 * implementation.
	 */
	void startTasks();

	/**
	 * Stop the running tasks.
	 */
	void stopTasks();

	/**
	 * Activate statistics for this scheduler instance
	 * 
	 * @param folder
	 *            The folder to save the statistics to
	 * @param executionId
	 *            The ID of the execution
	 */
	void activateStatistics(String folder, String executionId);
}
