package scheduling;

import common.Active;
import common.util.StopJvmUncaughtExceptionHandler;

/**
 * Thread that can be stopped on demand.
 * 
 * @author palivosd
 *
 */
public abstract class ActiveThread extends Thread implements Active {

	public ActiveThread() {
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

}
