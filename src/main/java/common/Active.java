package common;

//FIXME: Rename to enable/disable
public interface Active {
	default public void activate() {
	}

	default public boolean isActive() {
		return true;
	}

	default public void deActivate() {

	}
}
