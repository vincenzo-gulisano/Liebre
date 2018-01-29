package common.util;

public enum StopJvmUncaughtExceptionHandler implements Thread.UncaughtExceptionHandler {
	INSTANCE;
	@Override
	public void uncaughtException(Thread t, Throwable e) {
		System.err.format("[ERROR] %s crashed: ", t);
		e.printStackTrace();
		System.err.format("[ERROR] Liebre shutting down...%n");
		System.exit(1);
	}

}
