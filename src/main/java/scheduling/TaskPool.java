package scheduling;

public interface TaskPool<T extends Runnable> {
	Runnable getNext(long threadId);

	void put(T task);
}
