package dummy;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import scheduling.TaskPool;

/**
 * (Blocking) First-in, first-out {@link TaskPool} implementation with no
 * priority logic.
 * 
 * @author palivosd
 *
 */
public class FifoTaskPool implements TaskPool<Runnable> {

	private final Queue<Runnable> tasks = new LinkedBlockingQueue<>();

	@Override
	public Runnable getNext(long threadId) {
		return tasks.remove();
	}

	@Override
	public void put(Runnable task) {
		tasks.add(task);
	}

}
