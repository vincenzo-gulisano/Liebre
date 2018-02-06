package common;

public interface ActiveRunnable extends Active, Runnable, NamedEntity {
	void onScheduled();

	void onRun();
}
