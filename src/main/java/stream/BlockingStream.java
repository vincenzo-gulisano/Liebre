package stream;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import common.ActiveRunnable;
import common.tuple.Tuple;

public class BlockingStream<T extends Tuple> implements Stream<T> {

	private static final int CAPACITY = 10000;
	private static AtomicInteger nextIndex = new AtomicInteger();

	private final String id;
	private final int index;

	private BlockingQueue<T> stream = new LinkedBlockingQueue<T>(CAPACITY);
	private volatile long tuplesWritten, tuplesRead;
	private volatile boolean enabled;
	private final ActiveRunnable source;
	private final ActiveRunnable destination;

	public BlockingStream(String id, ActiveRunnable source, ActiveRunnable destination) {
		this.id = id;
		this.index = nextIndex.getAndIncrement();
		this.source = source;
		this.destination = destination;
		tuplesWritten = 0;
		tuplesRead = 0;
	}

	@Override
	public void addTuple(T tuple) {
		if (!isEnabled()) {
			return;
		}
		try {
			stream.put(tuple);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			disable();
		}
		tuplesWritten++;
	}

	@Override
	public T getNextTuple() {
		if (!isEnabled()) {
			return null;
		}
		T nextTuple = null;
		try {
			nextTuple = stream.take();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			disable();
		}
		tuplesRead++;
		return nextTuple;
	}

	@Override
	public void enable() {
		this.enabled = true;
	}

	@Override
	public boolean isEnabled() {
		return enabled;
	}

	@Override
	public void disable() {
		this.enabled = false;
	}

	@Override
	public T peek() {
		return isEnabled() ? stream.peek() : null;
	}

	@Override
	public long size() {
		return tuplesWritten - tuplesRead;
	}

	@Override
	public long remainingCapacity() {
		return stream.remainingCapacity();
	}

	@Override
	public String getId() {
		return this.id;
	}

	public ActiveRunnable getSource() {
		return source;
	}

	public ActiveRunnable getDestination() {
		return destination;
	}

	@Override
	public int getIndex() {
		return index;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof BlockingStream))
			return false;
		BlockingStream<T> other = (BlockingStream<T>) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "BlockingStream [id=" + id + ", stream=" + stream + ", tuplesWritten=" + tuplesWritten + ", tuplesRead="
				+ tuplesRead + "]";
	}

}
