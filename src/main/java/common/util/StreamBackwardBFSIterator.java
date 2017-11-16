package common.util;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import common.StreamConsumer;
import common.StreamProducer;
import operator.Operator;

//FIXME: Better semantics, this now only works for operators
public class StreamBackwardBFSIterator implements Iterator<Operator<?, ?>> {
	private final Queue<StreamConsumer<?>> queue = new LinkedList<>();

	public StreamBackwardBFSIterator(StreamConsumer<?> start) {
		queueAllPrevious(start);
	}

	@Override
	public boolean hasNext() {
		return !queue.isEmpty();
	}

	@Override
	public Operator<?, ?> next() {
		// FIXME: Some instanceof checks
		StreamConsumer<?> next = queue.remove();
		queueAllPrevious(next);
		return (Operator<?, ?>) next;
	}

	private void queueAllPrevious(StreamConsumer<?> consumer) {
		for (StreamProducer<?> prev : consumer.getPrevious()) {
			if (prev instanceof StreamConsumer<?>) {
				queue.add((StreamConsumer<?>) prev);
			}
		}
	}

}
