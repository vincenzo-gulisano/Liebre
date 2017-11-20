package scheduling;

import common.Active;

/**
 * Thread that can be stopped on demand.
 * 
 * @author palivosd
 *
 */
// TODO: Use interrupt mechanism instead of this.
public abstract class ActiveThread extends Thread implements Active {

	private volatile boolean active;

	@Override
	public void run() {
		while (active) {
			doRun();
		}
	}

	protected abstract void doRun();

	@Override
	public void enable() {
		this.active = true;
	}

	@Override
	public boolean isEnabled() {
		return this.active;
	}

	@Override
	public void disable() {
		this.active = false;
	}

}
