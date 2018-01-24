package scheduling;

import common.Active;

/**
 * Thread that can be stopped on demand.
 * 
 * @author palivosd
 *
 */
public abstract class ActiveThread extends Thread implements Active {

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
