package scheduling.thread;

import java.util.concurrent.TimeUnit;

import common.ActiveRunnable;
import scheduling.TaskPool;
import source.Source;

public class SourceThread extends ActiveThread {

	private final ActiveRunnable source;
	private final TaskPool<ActiveRunnable> pool;
	private final long quantumNanos;
	private final int index;

	public SourceThread(int index, ActiveRunnable source, TaskPool<ActiveRunnable> pool, long quantum, TimeUnit unit) {
		if (source instanceof Source<?> == false) {
			throw new IllegalArgumentException(
					String.format("%s only accept tasks of type Source", getClass().getSimpleName()));
		}
		this.index = index;
		this.source = source;
		this.pool = pool;
		this.quantumNanos = unit.toNanos(quantum);
	}

	@Override
	protected void doRun() {
		source.onScheduled();
		final long runUntil = System.nanoTime() + quantumNanos;
		while (System.nanoTime() < runUntil) {
			source.run();
		}
		pool.update(source, index);
	}

}
