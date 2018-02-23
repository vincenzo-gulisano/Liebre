package scheduling.thread;

public class BasicWorkerThread extends ActiveThread {

	private final Runnable task;

	public BasicWorkerThread(Runnable task) {
		this.task = task;
	}

	@Override
	public void doRun() {
		task.run();
	}
}
