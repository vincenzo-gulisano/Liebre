package common;

public interface Active {
	default public void enable() {
	}

	default public boolean isEnabled() {
		return true;
	}

	default public void disable() {

	}
}
