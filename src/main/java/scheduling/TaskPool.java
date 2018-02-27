package scheduling;

import common.Active;

public interface TaskPool<T extends Runnable> extends Active {
	default void register(T task) {
		put(task, -1);
	}

	default void registerPassive(T task) {
		System.out.format("[WARN] [%s] Ignoring registerPassive(%s)%n", getClass().getSimpleName(), task);
	}

	T getNext(int threadId);

	void put(T task, int threadId);

	void setThreadsNumber(int activeThreads);

}
