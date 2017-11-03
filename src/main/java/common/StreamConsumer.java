package common;

import java.util.Collection;

import common.tuple.Tuple;
import stream.Stream;

public interface StreamConsumer<IN extends Tuple> extends NamedEntity {
	void registerIn(StreamProducer<IN> in);

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

}
