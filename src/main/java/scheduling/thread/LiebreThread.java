package scheduling.thread;

import common.Active;
import common.util.StopJvmUncaughtExceptionHandler;

/**
 * Thread that can be stopped on demand.
 * 
 * @author palivosd
 *
 */
public abstract class LiebreThread extends Thread implements Active {
	private final int index;

	public LiebreThread() {
		this(-1);
	}

	public LiebreThread(int index) {
		this.index = index;
		setDefaultUncaughtExceptionHandler(StopJvmUncaughtExceptionHandler.INSTANCE);
	}

	@Override
	public void run() {
		while (isEnabled()) {
			doRun();
		}
	}

	protected abstract void doRun();

	@Override
	public void enable() {
	}

	@Override
	public boolean isEnabled() {
		return !isInterrupted();
	}

	@Override
	public void disable() {
		interrupt();
	}

	public int getIndex() {
		return index;
	}

}
