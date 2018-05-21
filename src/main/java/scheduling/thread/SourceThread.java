package scheduling.thread;

import java.util.concurrent.TimeUnit;

import common.component.Component;
import source.Source;

public class SourceThread extends LiebreThread {

	private final Component source;
	private final long quantumNanos;

	public SourceThread(int index, Component source, long quantum, TimeUnit unit) {
		super(index);
		if (source instanceof Source<?> == false) {
			throw new IllegalArgumentException(
					String.format("%s only accept tasks of type Source", getClass().getSimpleName()));
		}
		this.source = source;
		this.quantumNanos = unit.toNanos(quantum);
	}

	@Override
	protected void doRun() {
		source.onScheduled();
		final long runUntil = System.nanoTime() + quantumNanos;
		while (System.nanoTime() < runUntil) {
			source.run();
		}
		source.onRun();
	}

}
