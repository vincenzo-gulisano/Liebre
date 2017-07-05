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

import operator.BaseOperator;
import tuple.RichTuple;

public class TimeBasedNonAlignedSingleWindowAggregate<T1 extends RichTuple, T2 extends RichTuple>
	extends BaseOperator<T1, T2> {

    private double WS;
    private double WA;
    private TimeBasedSingleWindow<T1, T2> aggregateWindow;

    HashMap<String, LinkedList<T1>> tuples;
    HashMap<String, TimeBasedSingleWindow<T1, T2>> windows;
    HashMap<String, Double> windowsEarliestTimestamps;

    public TimeBasedNonAlignedSingleWindowAggregate(double WS, double WA,
	    TimeBasedSingleWindow<T1, T2> aggregateWindow) {
	tuples = new HashMap<String, LinkedList<T1>>();
	windows = new HashMap<String, TimeBasedSingleWindow<T1, T2>>();
	windowsEarliestTimestamps = new HashMap<String, Double>();
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

	double earliestWinStartTSforT = getEarliestWinStartTS(t.getTS());
	String key = t.getKey();

	/*
	 * If a window for the key exist, check whether stale tuples exist,
	 * purge them and then shift the window
	 */
	if (windows.containsKey(key)) {
	    // Assumption: if windows contains the key, then there is at least a
	    // tuple and an earliest timestamp !!!

	    double earliestWinStartTS = windowsEarliestTimestamps.get(key);

	    T2 outTuple = null;

	    while (earliestWinStartTS < earliestWinStartTSforT) {

		outTuple = windows.get(key).getAggregatedResult(
			earliestWinStartTS, t);

		// purge stale tuples
		while (tuples.get(key).size() > 0) {
		    T1 tuple = tuples.get(key).peek();
		    if (tuple.getTS() < earliestWinStartTS + WA) {
			windows.get(key).remove(tuple);
			tuples.get(key).pop();
		    } else {
			break;
		    }
		}
		// Shift the window
		windowsEarliestTimestamps.put(key, earliestWinStartTS + WA);
		earliestWinStartTS = windowsEarliestTimestamps.get(key);
	    }

	    if (outTuple != null)
		result.add(outTuple);
	}

	// Add contribution of this tuple
	if (!windows.containsKey(key)) {
	    windows.put(
		    key,
		    aggregateWindow.factory((long) earliestWinStartTSforT,
			    t.getKey()));
	    windowsEarliestTimestamps.put(key, earliestWinStartTSforT);
	    tuples.put(key, new LinkedList<T1>());
	}

	windows.get(key).add(t);

	// Store tuple
	tuples.get(key).add(t);

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
