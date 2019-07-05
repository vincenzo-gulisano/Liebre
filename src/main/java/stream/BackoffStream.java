/*
 * Copyright (C) 2017-2019
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package stream;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.lang3.builder.ToStringBuilder;

import common.tuple.RichTuple;
import common.tuple.Tuple;
import common.util.backoff.Backoff;
import common.util.backoff.BackoffFactory;
import component.StreamConsumer;
import component.StreamProducer;

/**
 * Stream implementation that has an (optional) approximate capacity that is
 * enforced by an optional provided {@link Backoff} strategy. The backoff
 * strategy is also activated in case the reader is faster than the writer, to
 * prevent spinning.
 *
 * @param <T>
 *            The type of tuples transferred by this {@link Stream}.
 * @see StreamFactory
 * @see common.util.backoff.ExponentialBackoff
 * @see common.util.backoff.NoopBackoff
 */
public class BackoffStream<T extends Tuple> implements Stream<T> {

	private static final double EMA_ALPHA = 0.5;
	private final Queue<T> stream = new ConcurrentLinkedQueue<>();
	private final int capacity;
	private final String id;
	private final int index;
	private final int relativeProducerIndex;
	private final int relativeConsumerIndex;
	private final StreamProducer<T> source;
	private final StreamConsumer<T> destination;
	private final Backoff readBackoff;
	private final Backoff writeBackoff;
	private volatile boolean enabled;
	private volatile long tuplesRead;
	private volatile long tuplesWritten;

	private volatile double averageArrivalTime = -1;

	/**
	 * Construct.
	 *
	 * @param id
	 *            The unique ID of the stream.
	 * @param index
	 *            The unique index of the stream.
	 * @param source
	 *            The producer
	 * @param destination
	 *            The consumer
	 * @param capacity
	 *            The capacity that the stream will try to enforce with the
	 *            {@link Backoff} strategy.
	 * @param backoff
	 *            The backoff strategy.
	 */
	BackoffStream(String id, int index, int relativeProducerIndex, int relativeConsumerIndex, StreamProducer<T> source,
			StreamConsumer<T> destination, int capacity, BackoffFactory backoff) {
		this.capacity = capacity;
		this.id = id;
		this.index = index;
		this.relativeProducerIndex=relativeProducerIndex;
		this.relativeConsumerIndex=relativeConsumerIndex;
		this.source = source;
		this.destination = destination;
		this.readBackoff = backoff.newInstance();
		this.writeBackoff = backoff.newInstance();
	}

	/**
	 * Get a {@link StreamFactory} implementation for {@link BackoffStream}s.
	 *
	 * @return A factory that generates {@link BackoffStream}s.
	 */
//	public static StreamFactory factory() {
//		return Factory.INSTANCE;
//	}

	@Override
	public void addTuple(T tuple, int writer) {
		if (offer(tuple,writer)) {
			writeBackoff.relax();
			return;
		}
		writeBackoff.backoff();
	}

	@Override
	public final boolean offer(T tuple, int writer) {
		stream.offer(tuple);
		tuplesWritten++;
		// FIXME: This should only run when scheduling is enabled!!
		if (tuple instanceof RichTuple) {
			long arrivalTime = ((RichTuple) tuple).getStimulus();
			averageArrivalTime = averageArrivalTime < 0 ? arrivalTime
					: ((EMA_ALPHA * arrivalTime) + ((1 - EMA_ALPHA) * averageArrivalTime));
		}
		return remainingCapacity() > 0;
	}

	@Override
	public T getNextTuple(int reader) {
		T tuple = poll(reader);
		if (tuple != null) {
			readBackoff.relax();
			return tuple;
		}
		readBackoff.backoff();
		return null;
	}

	@Override
	public final T poll(int reader) {
		T tuple = stream.poll();
		if (tuple != null) {
			tuplesRead++;
		}
		return tuple;
	}

	@Override
	public final T peek(int reader) {
		return stream.peek();
	}

	@Override
	public final int remainingCapacity() {
		return Math.max(capacity - size(), 0);
	}

	@Override
	public final int size() {
		return (int) (tuplesWritten - tuplesRead);
	}

	@SuppressWarnings("unchecked")
	@Override
	public StreamProducer<T>[] getSources() {
		return (StreamProducer<T>[]) new Object[] { source };
	}

	@SuppressWarnings("unchecked")
	@Override
	public StreamConsumer<T>[] getDestinations() {
		return (StreamConsumer<T>[]) new Object[] { destination };
	}

	@Override
	public void resetArrivalTime() {
		averageArrivalTime = -1;
	}

	@Override
	public double getAverageArrivalTime() {
		return averageArrivalTime;
	}

	@Override
	public void enable() {
		enabled = true;
	}

	@Override
	public boolean isEnabled() {
		return enabled;
	}

	@Override
	public void disable() {
		enabled = false;
	}

	@Override
	public String getId() {
		return id;
	}

	@Override
	public int getIndex() {
		return index;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this).append("id", id)
				.append("index", index).append("capacity", capacity)
				.append("size", size()).append("component/source", source)
				.append("destination", destination).append("enabled", enabled)
				.toString();
	}

//	/**
//	 * Default factory implementation, that enforces unique increasing integer
//	 * indexes.
//	 */
//	private enum Factory implements StreamFactory {
//		INSTANCE;
//
//		private final AtomicInteger indexes = new AtomicInteger();
//
//		@Override
//		public <T extends Tuple> Stream<T> newStream(StreamProducer<T> from,
//				StreamConsumer<T> to, int capacity, BackoffFactory backoff) {
//			return new BackoffStream<>(getStreamId(from, to),
//					indexes.getAndIncrement(), from, to, capacity, backoff);
//		}
//
//		@Override
//		public <T extends RichTuple> MWMRSortedStream<T> newMWMRSortedStream(
//				StreamProducer<T>[] sources, StreamConsumer<T>[] destinations,
//				int maxLevels) {
//			throw new UnsupportedOperationException(
//					"BackoffStream cannot create MWMRSortedStreams");
//		}
//	}

	@Override
	public int getRelativeProducerIndex() {
		return relativeProducerIndex;
	}

	@Override
	public int getRelativeConsumerIndex() {
		return relativeConsumerIndex;
	}
}
