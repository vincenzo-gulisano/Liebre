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

package operator.aggregate;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import common.tuple.RichTuple;
import operator.in1.BaseOperator1In;
import stream.StreamFactory;

public class TimeBasedSingleWindowAggregate<T1 extends RichTuple, T2 extends RichTuple> extends BaseOperator1In<T1, T2> {

	private long WS;
	private long WA;
	private TimeBasedSingleWindow<T1, T2> aggregateWindow;

	private long latestTimestamp;
	private boolean firstTuple = true;

	LinkedList<T1> tuples;
	TreeMap<Long, HashMap<String, TimeBasedSingleWindow<T1, T2>>> windows;
	TreeMap<Long, HashMap<String, WinCounter>> windowsCounters;

	private class WinCounter {
		private long count = 0;

		public void add(long v) {
			count += v;
		}

		public boolean isZero() {
			return count == 0;
		}
	}

	public TimeBasedSingleWindowAggregate(String id, StreamFactory streamFactory, long windowSize, long windowSlide,
			TimeBasedSingleWindow<T1, T2> aggregateWindow) {
		super(id, streamFactory);
		tuples = new LinkedList<T1>();
		windows = new TreeMap<Long, HashMap<String, TimeBasedSingleWindow<T1, T2>>>();
		windowsCounters = new TreeMap<Long, HashMap<String, WinCounter>>();
		this.WS = windowSize;
		this.WA = windowSlide;
		this.aggregateWindow = aggregateWindow;
	}

	public long getEarliestWinStartTS(long ts) {
		return (long) Math.max((Math.floor(ts / WA) - Math.ceil(WS / WA) + 1) * WA, 0.0);
	}

	public List<T2> processTupleIn1(T1 t) {

		List<T2> result = new LinkedList<T2>();

		// make sure timestamps are not decreasing
		if (firstTuple) {
			firstTuple = false;
		} else {
			if (t.getTimestamp() < latestTimestamp) {
				throw new RuntimeException("Input tuple's timestamp decreased!");
			}
		}
		latestTimestamp = t.getTimestamp();

		long earliestWinStartTSforT = getEarliestWinStartTS(t.getTimestamp());

		// Managing of stale windows
		boolean purgingNotDone = true;
		while (purgingNotDone && windows.size() > 0) {

			long earliestWinStartTS = windows.firstKey();

			if (earliestWinStartTS < earliestWinStartTSforT) {

				// Produce results for stale windows
				for (TimeBasedSingleWindow<T1, T2> w : windows.get(earliestWinStartTS).values()) {
					result.add(w.getAggregatedResult());
				}

				// Remove contribution of stale tuples from stale windows
				while (tuples.size() > 0) {
					T1 tuple = tuples.peek();
					if (tuple.getTimestamp() < earliestWinStartTS + WA) {

						windows.get(earliestWinStartTS).get(tuple.getKey()).remove(tuple);
						windowsCounters.get(earliestWinStartTS).get(tuple.getKey()).add(-1);

						if (windowsCounters.get(earliestWinStartTS).get(tuple.getKey()).isZero()) {
							windows.get(earliestWinStartTS).remove(tuple.getKey());
							windowsCounters.get(earliestWinStartTS).remove(tuple.getKey());
						}
						tuples.pop();

					} else {
						break;
					}
				}

				// Shift windows
				if (!windows.containsKey(earliestWinStartTS + WA)) {
					windows.put(earliestWinStartTS + WA, new HashMap<String, TimeBasedSingleWindow<T1, T2>>());
					windowsCounters.put(earliestWinStartTS + WA, new HashMap<String, WinCounter>());
				}
				windows.get(earliestWinStartTS + WA).putAll(windows.get(earliestWinStartTS));
				windowsCounters.get(earliestWinStartTS + WA).putAll(windowsCounters.get(earliestWinStartTS));
				for (TimeBasedSingleWindow<T1, T2> w : windows.get(earliestWinStartTS + WA).values()) {
					w.setStartTimestamp(earliestWinStartTS + WA);
				}
				windows.remove(earliestWinStartTS);
				windowsCounters.remove(earliestWinStartTS);
			} else {
				purgingNotDone = false;
			}
		}

		// Add contribution of this tuple
		if (!windows.containsKey(earliestWinStartTSforT)) {
			windows.put(earliestWinStartTSforT, new HashMap<String, TimeBasedSingleWindow<T1, T2>>());
			windowsCounters.put(earliestWinStartTSforT, new HashMap<String, WinCounter>());
		}
		if (!windows.get(earliestWinStartTSforT).containsKey(t.getKey())) {
			windows.get(earliestWinStartTSforT).put(t.getKey(), aggregateWindow.factory());
			windows.get(earliestWinStartTSforT).get(t.getKey()).setKey(t.getKey());
			windows.get(earliestWinStartTSforT).get(t.getKey()).setStartTimestamp(earliestWinStartTSforT);
			windowsCounters.get(earliestWinStartTSforT).put(t.getKey(), new WinCounter());
		}
		windows.get(earliestWinStartTSforT).get(t.getKey()).add(t);
		windowsCounters.get(earliestWinStartTSforT).get(t.getKey()).add(1);

		// Store tuple
		tuples.add(t);

		return result;

	}

}
