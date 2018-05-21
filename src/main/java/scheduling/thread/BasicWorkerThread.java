package scheduling.thread;

public class BasicWorkerThread extends LiebreThread {

	private final Runnable task;

	public BasicWorkerThread(Runnable task) {
		this.task = task;
	}

	@Override
	public void doRun() {
		task.run();
	}
}
