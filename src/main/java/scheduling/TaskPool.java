package scheduling;

public interface TaskPool<T extends Runnable> {
	T getNext(long threadId);

	void put(T task);
}
