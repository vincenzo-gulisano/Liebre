package scheduling;

import common.Active;

/**
 * Thread that can be stopped on demand.
 * 
 * @author palivosd
 *
 */
// FIXME: Use interrupt mechanism instead of this.
public abstract class ActiveThread extends Thread implements Active {

	private boolean active;

	@Override
	public void run() {
		while (active) {
			doRun();
		}
	}

	protected abstract void doRun();

	@Override
	public void activate() {
		this.active = true;
	}

	@Override
	public boolean isActive() {
		return this.active;
	}

	@Override
	public void deActivate() {
		this.active = false;
	}

}
