package stream;

import common.scalegate.ScaleGate;
import common.scalegate.ScaleGateAArrImpl;
import common.scalegate.TuplesFromAll;
import common.tuple.RichTuple;
import component.StreamConsumer;
import component.StreamProducer;

/*
 * Assumption: all writers and reader threads are distinct
 * (i.e., this class is not safe when used in combination
 * with custom thread scheduling)
 */
public class SGStream<T extends RichTuple> implements Stream<T> {

	// TODO Am I using id in the right way?
	// TODO Am I using index in the right way?
	// TODO Am I using enabled in the right way?
	// TODO Am I using sources in the right way?
	// TODO Am I using destinations in the right way?

	private String id;
	private int index;
	private final int relativeProducerIndex;
	private final int relativeConsumerIndex;
	private boolean enabled;
	private ScaleGate<T> sg;
	private StreamProducer<T>[] sources;
	private StreamConsumer<T>[] destinations;

	private TuplesFromAll barrier;

	public SGStream(String id, int index, int relativeProducerIndex,
			int relativeConsumerIndex, int maxLevels, int writers, int readers,
			StreamProducer<T>[] sources, StreamConsumer<T>[] destinations) {
		this.id = id;
		this.index = index;
		this.relativeProducerIndex = relativeProducerIndex;
		this.relativeConsumerIndex = relativeConsumerIndex;
		enabled = false;
		this.sg = new ScaleGateAArrImpl<T>(maxLevels, writers, readers);
		this.sources = sources;
		this.destinations = destinations;

		barrier = new TuplesFromAll();
		barrier.setSize(writers);

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
	public void addTuple(T tuple,int writer) {
		assert (enabled);
		sg.addTuple(tuple, writer);
	}

	@Override
	public T getNextTuple(int reader) {
		assert (enabled);
		while (barrier.receivedTupleFromEachInput()) {

		}
		return sg.getNextReadyTuple(reader);
	}

	@Override
	public StreamProducer<T>[] getSources() {
		return sources;
	}

	@Override
	public StreamConsumer<T>[] getDestinations() {
		return destinations;
	}

	@Override
	public int getRelativeProducerIndex() {
		return relativeProducerIndex;
	}

	@Override
	public void setRelativeProducerIndex(int index) {
		throw new UnsupportedOperationException(
				"relativeProducerIndex cannot be changed for a stream");
	}

	@Override
	public int getRelativeConsumerIndex() {
		return relativeConsumerIndex;
	}

	@Override
	public void setRelativeConsumerIndex(int index) {
		throw new UnsupportedOperationException(
				"setRelativeConsumerIndex cannot be changed for a stream");
	}

	@Override
	public boolean offer(T tuple, int writer) {
		throw new UnsupportedOperationException("cannot invoke this function on SGStream");
	}

	@Override
	public T poll(int reader) {
		throw new UnsupportedOperationException("cannot invoke this function on SGStream");
	}

	@Override
	public T peek(int reader) {
		throw new UnsupportedOperationException("cannot invoke this function on SGStream");
	}

	@Override
	public int remainingCapacity() {
		return Integer.MAX_VALUE;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public void resetArrivalTime() {
		throw new UnsupportedOperationException("cannot invoke this function on SGStream");
	}

	@Override
	public double getAverageArrivalTime() {
		throw new UnsupportedOperationException("cannot invoke this function on SGStream");
	}
}
