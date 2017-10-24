/*  Copyright (C) 2017  Vincenzo Gulisano
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
 *  Contact: Vincenzo Gulisano info@vincenzogulisano.com
 *
 */

package operator.in2.join;

import java.util.LinkedList;
import java.util.List;

import common.tuple.RichTuple;
import operator.in2.BaseOperator2In;

public class TimeBasedJoin<T1 extends RichTuple, T2 extends RichTuple, T3 extends RichTuple>
		extends BaseOperator2In<T1, T2, T3> {

	private long ws;

	private LinkedList<T1> in1Tuples;
	private LinkedList<T2> in2Tuples;

	Predicate<T1, T2, T3> predicate;

	// This is for determinism
	private LinkedList<T1> in1TuplesBuffer;
	private LinkedList<T2> in2TuplesBuffer;

	public TimeBasedJoin(String id, long windowSize, Predicate<T1, T2, T3> predicate) {
		super(id);
		this.ws = windowSize;
		this.predicate = predicate;

		in1Tuples = new LinkedList<T1>();
		in2Tuples = new LinkedList<T2>();

		in1TuplesBuffer = new LinkedList<T1>();
		in2TuplesBuffer = new LinkedList<T2>();
	}

	protected void purge(long ts) {
		while (in1Tuples.size() > 0 && in1Tuples.peek().getTimestamp() < ts - ws)
			in1Tuples.poll();
		while (in2Tuples.size() > 0 && in2Tuples.peek().getTimestamp() < ts - ws)
			in2Tuples.poll();
	}

	private List<T3> processReadyTuples() {

		List<T3> results = new LinkedList<T3>();

		while (in1buffered() && in2buffered()) {
			if (buffer1Peek().getTimestamp() < buffer2Peek().getTimestamp()) {

				T1 tuple = buffer1Poll();

				purge(tuple.getTimestamp());

				if (in2Tuples.size() > 0) {

					for (T2 t : in2Tuples) {
						T3 result = predicate.compare(tuple, t);
						if (result != null) {
							results.add(result);
						}

					}

				}

				in1Tuples.add(tuple);

			} else {

				T2 tuple = buffer2Poll();

				purge(tuple.getTimestamp());

				if (in1Tuples.size() > 0) {

					for (T1 t : in1Tuples) {
						T3 result = predicate.compare(t, tuple);
						if (result != null) {
							results.add(result);
						}

					}

				}

				in2Tuples.add(tuple);

			}
		}

		return results;

	}

	@Override
	public List<T3> processTupleIn1(T1 tuple) {

		in1buffer(tuple);
		return processReadyTuples();

	}

	@Override
	public List<T3> processTupleIn2(T2 tuple) {

		in2buffer(tuple);
		return processReadyTuples();

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

}
