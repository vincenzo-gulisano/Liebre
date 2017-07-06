package operator2in.join;

import java.util.LinkedList;
import java.util.List;
import operator2in.BaseOperator2In;
import tuple.RichTuple;

public class TimeBasedJoin<T1 extends RichTuple, T2 extends RichTuple, T3 extends RichTuple>
		extends BaseOperator2In<T1, T2, T3> {

	private long ws;

	private LinkedList<T1> in1Tuples;
	private LinkedList<T2> in2Tuples;

	Predicate<T1, T2, T3> predicate;

	// This is for determinism
	private LinkedList<T1> in1TuplesBuffer;
	private LinkedList<T2> in2TuplesBuffer;

	public TimeBasedJoin(long windowSize, Predicate<T1, T2, T3> predicate) {
		this.ws = windowSize;
		this.predicate = predicate;

		in1Tuples = new LinkedList<T1>();
		in2Tuples = new LinkedList<T2>();

		in1TuplesBuffer = new LinkedList<T1>();
		in2TuplesBuffer = new LinkedList<T2>();
	}

	protected void process() {
		T1 inTuple1 = in1.getNextTuple();
		while (inTuple1 != null) {
			in1buffer(inTuple1);
			inTuple1 = in1.getNextTuple();
		}
		T2 inTuple2 = in2.getNextTuple();
		while (inTuple2 != null) {
			in2buffer(inTuple2);
			inTuple2 = in2.getNextTuple();
		}

		while (in1buffered() && in2buffered()) {
			if (buffer1Peek().getTimestamp() < buffer2Peek().getTimestamp()) {
				List<T3> outTuples = processTupleIn1(buffer1Poll());
				if (outTuples != null) {
					for (T3 t : outTuples)
						out.addTuple(t);
				}
			} else {
				List<T3> outTuples = processTupleIn2(buffer2Poll());
				if (outTuples != null) {
					for (T3 t : outTuples)
						out.addTuple(t);
				}
			}
		}
	}

	@Override
	protected List<T3> processTupleIn1(T1 tuple) {

		List<T3> results = new LinkedList<T3>();

		while (in2Tuples.size() > 0
				&& in2Tuples.peek().getTimestamp() < tuple.getTimestamp() - ws)
			in2Tuples.poll();

		if (in2Tuples.size() > 0) {

			for (T2 t : in2Tuples) {
				T3 result = predicate.compare(tuple, t);
				if (result != null) {
					results.add(result);
				}

			}

		}

		in1Tuples.add(tuple);

		return results;
	}

	private boolean in1buffered() {
		return !in1TuplesBuffer.isEmpty();
	}

	private boolean in2buffered() {
		return !in2TuplesBuffer.isEmpty();
	}

	private void in1buffer(T1 t) {
		in1TuplesBuffer.add(t);
	}

	private void in2buffer(T2 t) {
		in2TuplesBuffer.add(t);
	}

	private T1 buffer1Peek() {
		return in1TuplesBuffer.peek();
	}

	private T2 buffer2Peek() {
		return in2TuplesBuffer.peek();
	}

	private T1 buffer1Poll() {
		return in1TuplesBuffer.poll();
	}

	private T2 buffer2Poll() {
		return in2TuplesBuffer.poll();
	}

	@Override
	protected List<T3> processTupleIn2(T2 tuple) {
		List<T3> results = new LinkedList<T3>();

		while (in1Tuples.size() > 0
				&& in1Tuples.peek().getTimestamp() < tuple.getTimestamp() - ws)
			in1Tuples.poll();

		if (in1Tuples.size() > 0) {

			for (T1 t : in1Tuples) {
				T3 result = predicate.compare(t, tuple);
				if (result != null) {
					results.add(result);
				}

			}

		}

		in2Tuples.add(tuple);

		return results;
	}

}
