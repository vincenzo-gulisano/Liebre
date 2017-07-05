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

import operator.BaseOperator;
import tuple.RichTuple;

public class TimeBasedSingleWindowAggregate<T1 extends RichTuple, T2 extends RichTuple>
	extends BaseOperator<T1, T2> {

    private double WS;
    private double WA;
    private TimeBasedSingleWindow<T1, T2> aggregateWindow;

    private double latestTimestamp;
    private boolean firstTuple = true;

    LinkedList<T1> tuples;
    TreeMap<Double, HashMap<String, TimeBasedSingleWindow<T1, T2>>> windows;

    public TimeBasedSingleWindowAggregate(double WS, double WA,
	    TimeBasedSingleWindow<T1, T2> aggregateWindow) {
	tuples = new LinkedList<T1>();
	windows = new TreeMap<Double, HashMap<String, TimeBasedSingleWindow<T1, T2>>>();
	this.WS = WS;
	this.WA = WA;
	this.aggregateWindow = aggregateWindow;
    }

    public double getEarliestWinStartTS(double ts) {
	return Math.max((Math.floor(ts / WA) - Math.ceil(WS / WA) + 1) * WA,
		0.0);
    }

    public List<T2> processTuple(T1 t) {

	List<T2> result = new LinkedList<T2>();

	// make sure timestamps are not decreasing
	if (firstTuple) {
	    firstTuple = false;
	} else {
	    if (t.getTS() < latestTimestamp) {
		throw new RuntimeException("Input tuple's timestamp decreased!");
	    }
	}
	latestTimestamp = t.getTS();

	double earliestWinStartTSforT = getEarliestWinStartTS(t.getTS());

	// Managing of stale windows
	boolean purgingNotDone = true;
	while (purgingNotDone && windows.size() > 0) {

	    double earliestWinStartTS = windows.firstKey();

	    if (earliestWinStartTS < earliestWinStartTSforT) {

		// Produce results for stale windows
		for (TimeBasedSingleWindow<T1, T2> w : windows.get(
			earliestWinStartTS).values()) {
		    result.add(w.getAggregatedResult(earliestWinStartTS, t));
		}

		// Remove contribution of stale tuples from stale windows
		while (tuples.size() > 0) {
		    T1 tuple = tuples.peek();
		    if (tuple.getTS() < earliestWinStartTS + WA) {

			windows.get(earliestWinStartTS).get(tuple.getKey())
				.remove(tuple);

			if (windows.get(earliestWinStartTS).get(tuple.getKey())
				.size() == 0)
			    windows.get(earliestWinStartTS).remove(
				    tuple.getKey());
			tuples.pop();

		    } else {
			break;
		    }
		}

		// Shift windows
		if (!windows.containsKey(earliestWinStartTS + WA))
		    windows.put(
			    earliestWinStartTS + WA,
			    new HashMap<String, TimeBasedSingleWindow<T1, T2>>());
		windows.get(earliestWinStartTS + WA).putAll(
			windows.get(earliestWinStartTS));
		windows.remove(earliestWinStartTS);

	    } else {
		purgingNotDone = false;
	    }
	}

	// Add contribution of this tuple
	if (!windows.containsKey(earliestWinStartTSforT))
	    windows.put(earliestWinStartTSforT,
		    new HashMap<String, TimeBasedSingleWindow<T1, T2>>());
	if (!windows.get(earliestWinStartTSforT).containsKey(t.getKey())) {
	    windows.get(earliestWinStartTSforT).put(
		    t.getKey(),
		    aggregateWindow.factory((long) earliestWinStartTSforT,
			    t.getKey()));
	}
	windows.get(earliestWinStartTSforT).get(t.getKey()).add(t);

	// Store tuple
	tuples.add(t);

	return result;

    }

    @Override
    protected void process() {
	T1 inTuple = in.getNextTuple();
	if (inTuple != null) {
	    for (T2 outT : processTuple(inTuple))
		out.addTuple(outT);
	}
    }
}
