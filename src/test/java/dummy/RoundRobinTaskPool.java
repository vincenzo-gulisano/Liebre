package dummy;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.LongAdder;

import operator.Operator;
import scheduling.TaskPool;

/**
 * (Blocking) First-in, first-out {@link TaskPool} implementation with no
 * priority logic.
 * 
 * @author palivosd
 *
 */
public class RoundRobinTaskPool implements TaskPool<Operator<?, ?>> {

	private final LinkedBlockingQueue<Operator<?, ?>> tasks = new LinkedBlockingQueue<>();
	private final ConcurrentHashMap<String, LongAdder> executions = new ConcurrentHashMap<>();

	@Override
	public Operator<?, ?> getNext(long threadId) {
		try {
			Operator<?, ?> task = tasks.take();
			// Log execution
			executions.computeIfAbsent(task.getId(), k -> new LongAdder()).increment();
			return task;
		} catch (InterruptedException exception) {
			exception.printStackTrace();
			throw new IllegalStateException();
		}

	}

	@Override
	public void put(Operator<?, ?> task) {
		tasks.add(task);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("*** [FifoTaskPool]\n");
		sb.append("Executions per operator: ").append(executions.toString()).append("\n");
		sb.append("Current Operator States: ").append(tasks.toString()).append("\n");
		return sb.toString();
	}

	@Override
	public void enable() {

	}

	@Override
	public boolean isEnabled() {
		return true;
	}

	@Override
	public void disable() {

	}

}
