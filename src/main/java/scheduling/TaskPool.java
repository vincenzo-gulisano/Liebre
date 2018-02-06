package scheduling;

import common.Active;

public interface TaskPool<T extends Runnable> extends Active {
	default void register(T task) {
		put(task);
	}

	T getNext(long threadId);

	void put(T task);

}
