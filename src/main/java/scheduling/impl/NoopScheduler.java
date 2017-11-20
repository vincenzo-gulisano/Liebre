package scheduling.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import operator.Operator;
import scheduling.ActiveThread;
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
	private final List<OneToOneWorkerThread> threads = new ArrayList<>();

	private static class OneToOneWorkerThread extends ActiveThread {

		private final Runnable task;

		public OneToOneWorkerThread(Runnable task) {
			this.task = task;
		}

		@Override
		public void doRun() {
			task.run();
		}

	}

	@Override
	public void addTasks(Collection<? extends Operator<?, ?>> tasks) {
		operators.addAll(tasks);
	}

	@Override
	public void startTasks() {
		for (Runnable operator : operators) {
			OneToOneWorkerThread thread = new OneToOneWorkerThread(operator);
			threads.add(thread);
			thread.enable();
			thread.start();
		}
	}

	@Override
	public void stopTasks() {
		for (OneToOneWorkerThread thread : threads) {
			try {
				thread.disable();
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

}
