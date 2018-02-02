package scheduling.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import operator.Operator;
import scheduling.Scheduler;

/**
 * Scheduler implementation in case no scheduling is actually needed and the
 * requirement is just one thread per operator.
 * 
 * @author palivosd
 *
 */
public class NoopScheduler implements Scheduler {
	private final List<Runnable> operators = new ArrayList<>();
	private final List<BasicWorkerThread> threads = new ArrayList<>();

	@Override
	public void addTasks(Collection<? extends Operator<?, ?>> tasks) {
		operators.addAll(tasks);
	}

	@Override
	public void startTasks() {
		for (Runnable operator : operators) {
			BasicWorkerThread thread = new BasicWorkerThread(operator);
			threads.add(thread);
			thread.enable();
			thread.start();
		}
	}

	@Override
	public void stopTasks() {
		for (BasicWorkerThread thread : threads) {
			try {
				thread.disable();
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
				Thread.currentThread().interrupt();
			}
		}
	}

	@Override
	public void activateStatistics(String folder, String executionId) {
		System.out.format("*** [%s] No statistics available%n", getClass().getSimpleName());
	}

}
