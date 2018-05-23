/*  Copyright (C) 2017-2018  Vincenzo Gulisano, Dimitris Palyvos Giannas
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *  Contact:
 *    Vincenzo Gulisano info@vincenzogulisano.com
 *    Dimitris Palyvos Giannas palyvos@chalmers.se
 */

package common;

import common.component.Component;
import java.util.Collection;

import common.tuple.Tuple;
import stream.Stream;

/**
 * A stream {@link Component} that consumes tuples.
 * @param <IN> The input type for this component.
 */
public interface StreamConsumer<IN extends Tuple> extends Named, Component {

  /**
   * FIXME: This should not be public if possible
   * Register an input for this consumer.
   * @param in The writer that will be connected to this reader.
   */
	void registerIn(StreamProducer<IN> in);

  /**
   * Get the upstream Components that are directly connected to this.
   * @return
   */
	Collection<StreamProducer<?>> getPrevious();

	/**
	 * Get the input {@link Stream} of this {@link StreamConsumer}. If the instance
	 * has multiple input {@link Stream}s, then the stream connected to the given
	 * entity id is returned.
	 * 
	 * @param requestorId
	 *            The unique ID of the {@link StreamProducer} that is connected to
	 *            this input stream. This is only used in cases where the operator
	 *            has more than one input streams and we need to know both ends of
	 *            the stream to return it correctly.
	 * @return The input {@link Stream} of this {@link StreamConsumer}.
	 */
	Stream<IN> getInputStream(String requestorId);

	/**
	 * Heuristic that indicates that the {@link StreamConsumer} has some input
	 * <b>all</b> its input streams. Might not always be accurate in the case of
	 * operators with multiple input streams.
	 * 
	 * @return {@code true} if the operator has some tuples available on all its
	 *         input streams.
	 */
	boolean hasInput();

}
