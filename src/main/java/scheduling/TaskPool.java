package scheduling;

import common.Active;

public interface TaskPool<T extends Runnable> extends Active {
	default void register(T task) {
		put(task, -1);
	}

	T getNext(int threadId);

	void put(T task, int threadId);

}
