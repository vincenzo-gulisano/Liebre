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

package operator2in.join;

import java.util.LinkedList;
import java.util.List;
import operator2in.BaseOperator2In;
import tuple.RichTuple;

public class TimeBasedJoin<T1 extends RichTuple, T2 extends RichTuple, T3 extends RichTuple>
		extends BaseOperator2In<T1, T2, T3> {

	private long ws;

	private LinkedList<T1> leftWin;
	private LinkedList<T2> rightWin;

	Predicate<T1, T2, T3> predicate;

	// This is for determinism
	// private LinkedList<T1> in1TuplesBuffer;
	// private LinkedList<T2> in2TuplesBuffer;

	public TimeBasedJoin(long windowSize, Predicate<T1, T2, T3> predicate) {
		this.ws = windowSize;
		this.predicate = predicate;

		leftWin = new LinkedList<T1>();
		rightWin = new LinkedList<T2>();

		// in1TuplesBuffer = new LinkedList<T1>();
		// in2TuplesBuffer = new LinkedList<T2>();
	}

	protected void purgeLeftWin(long ts) {
		while (leftWin.size() > 0 && leftWin.peek().getTimestamp() < ts - ws)
			leftWin.poll();
	}

	protected void purgeRightWin(long ts) {
		while (rightWin.size() > 0 && rightWin.peek().getTimestamp() < ts - ws)
			rightWin.poll();
	}

//	private List<T3> processReadyTuples() {
//
//		List<T3> results = new LinkedList<T3>();
//
//		while (in1buffered() && in2buffered()) {
//			if (buffer1Peek().getTimestamp() < buffer2Peek().getTimestamp()) {
//
//				T1 tuple = buffer1Poll();
//
//				purge(tuple.getTimestamp());
//
//				if (rightWin.size() > 0) {
//
//					for (T2 t : rightWin) {
//						T3 result = predicate.compare(tuple, t);
//						if (result != null) {
//							results.add(result);
//						}
//
//					}
//
//				}
//
//				leftWin.add(tuple);
//
//			} else {
//
//				T2 tuple = buffer2Poll();
//
//				purge(tuple.getTimestamp());
//
//				if (leftWin.size() > 0) {
//
//					for (T1 t : leftWin) {
//						T3 result = predicate.compare(t, tuple);
//						if (result != null) {
//							results.add(result);
//						}
//
//					}
//
//				}
//
//				rightWin.add(tuple);
//
//			}
//		}
//
//		return results;
//
//	}

	// LEFT
	@Override
	public List<T3> processTupleIn1(T1 tuple) {

		List<T3> results = new LinkedList<T3>();

		purgeRightWin(tuple.getTimestamp());

		if (rightWin.size() > 0) {

			for (T2 t : rightWin) {
				T3 result = predicate.compare(tuple, t);
				if (result != null) {
					results.add(result);
				}

			}

		}

		leftWin.add(tuple);

		return results;

	}

	// RIGHT
	@Override
	public List<T3> processTupleIn2(T2 tuple) {

		List<T3> results = new LinkedList<T3>();

		purgeLeftWin(tuple.getTimestamp());

		if (leftWin.size() > 0) {

			for (T1 t : leftWin) {
				T3 result = predicate.compare(t, tuple);
				if (result != null) {
					results.add(result);
				}

			}

		}

		rightWin.add(tuple);

		return results;

	}

	// private boolean in1buffered() {
	// return !in1TuplesBuffer.isEmpty();
	// }
	//
	// private boolean in2buffered() {
	// return !in2TuplesBuffer.isEmpty();
	// }
	//
	// private void in1buffer(T1 t) {
	// in1TuplesBuffer.add(t);
	// }
	//
	// private void in2buffer(T2 t) {
	// in2TuplesBuffer.add(t);
	// }
	//
	// private T1 buffer1Peek() {
	// return in1TuplesBuffer.peek();
	// }
	//
	// private T2 buffer2Peek() {
	// return in2TuplesBuffer.peek();
	// }
	//
	// private T1 buffer1Poll() {
	// return in1TuplesBuffer.poll();
	// }
	//
	// private T2 buffer2Poll() {
	// return in2TuplesBuffer.poll();
	// }

}
