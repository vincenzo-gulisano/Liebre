package stream;

import common.scalegate.ScaleGate;
import common.scalegate.ScaleGateAArrImpl;
import common.scalegate.TuplesFromAll;
import component.StreamConsumer;
import component.StreamProducer;
import java.util.List;

/*
 * Assumption: all writers and reader threads are distinct
 * (i.e., this class is not safe when used in combination
 * with custom thread scheduling)
 */
public class SGStream<T extends Comparable<? super T>> implements Stream<T> {

	// TODO Am I using id in the right way?
	// TODO Am I using index in the right way?
	// TODO Am I using enabled in the right way?
	// TODO Am I using sources in the right way?
	// TODO Am I using destinations in the right way?

	private String id;
	private int index;
	private boolean enabled;
	private ScaleGate<T> sg;
	private List<StreamProducer<T>> sources;
	private List<StreamConsumer<T>> destinations;

	private TuplesFromAll barrier;

	public SGStream(String id, int index,
			int maxLevels, int writers, int readers,
			List<StreamProducer<T>> sources, List<StreamConsumer<T>> destinations) {
		this.id = id;
		this.index = index;
		enabled = false;
		this.sg = new ScaleGateAArrImpl(maxLevels, writers, readers);
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
	public List<StreamProducer<T>> getSources() {
		return sources;
	}

	@Override
	public List<StreamConsumer<T>> getDestinations() {
		return destinations;
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
