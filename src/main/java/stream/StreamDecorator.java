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

import component.StreamConsumer;
import component.StreamProducer;
import common.tuple.Tuple;

/**
 * Basic decorator for {@link Stream} instances. Delegates all function cals to
 * the decorated instance.
 *
 * @param <T>
 *            The type of tuples transferred by the stream.
 */
public class StreamDecorator<T extends Tuple> implements Stream<T> {

	private final Stream<T> decorated;

	/**
	 * Construct.
	 *
	 * @param decorated
	 *            The stream to be decorated.
	 */
	public StreamDecorator(Stream<T> decorated) {
		this.decorated = decorated;
	}

	@Override
	public void enable() {
		decorated.enable();
	}

	@Override
	public boolean isEnabled() {
		return decorated.isEnabled();
	}

	@Override
	public void disable() {
		decorated.disable();
	}

	@Override
	public String getId() {
		return decorated.getId();
	}

	@Override
	public int getIndex() {
		return decorated.getIndex();
	}

	@Override
	public void addTuple(T tuple, int writer) {
		decorated.addTuple(tuple, writer);
	}

	@Override
	public T getNextTuple(int reader) {
		return decorated.getNextTuple(reader);
	}

	@Override
	public T peek(int reader) {
		return decorated.peek(reader);
	}

	@Override
	public int size() {
		return decorated.size();
	}

	@Override
	public int remainingCapacity() {
		return decorated.remainingCapacity();
	}

	@Override
	public boolean offer(T tuple, int writer) {
		return decorated.offer(tuple, writer);
	}

	@Override
	public T poll(int reader) {
		return decorated.poll(reader);
	}

	@Override
	public String toString() {
		return decorated.toString();
	}

	public StreamProducer<T>[] getSources() {
		return decorated.getSources();
	}

	public StreamConsumer<T>[] getDestinations() {
		return decorated.getDestinations();
	}

	@Override
	public void resetArrivalTime() {
		decorated.resetArrivalTime();
	}

	@Override
	public double getAverageArrivalTime() {
		return decorated.getAverageArrivalTime();
	}
	

	@Override
	public int getRelativeProducerIndex(int index) {
		return decorated.getRelativeProducerIndex(index);
	}

	@Override
	public int getRelativeConsumerIndex(int index) {
		return decorated.getRelativeConsumerIndex(index);
	}
}
