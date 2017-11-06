package common;

import java.util.Collection;

import common.tuple.Tuple;
import operator.in2.Operator2In;
import stream.Stream;

public interface StreamProducer<OUT extends Tuple> extends NamedEntity {
	void registerOut(StreamConsumer<OUT> out);

	default void registerOut(Operator2In<?, OUT, ?> out) {
		registerOut(out.secondInputView());
	}

	Collection<StreamConsumer<OUT>> getNext();

	/**
	 * Get the output {@link Stream} of this {@link StreamProducer}. If the instance
	 * has multiple output {@link Stream}s, then the stream connected to the given
	 * entity id is returned.
	 * 
	 * @param requestorId
	 *            The unique ID of the {@link StreamConsumer} that is connected to
	 *            this input stream. This is only used in cases where the operator
	 *            has more than one output streams and we need to know both ends of
	 *            the stream to return it correctly.
	 * @return The output {@link Stream} of this {@link StreamProducer}.
	 */
	Stream<OUT> getOutputStream(String requestorId);

}
