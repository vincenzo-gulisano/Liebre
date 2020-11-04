/*
 * Copyright (C) 2017-2019
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package component.operator.in1.aggregate;

import common.tuple.RichTuple;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

/**
 * Aggregate implementation for sliding time-based windows. Decides which tuples belong to which
 * windows and takes care of producing aggregation results by delegating to a provided {@link
 * TimeWindowAddRemove} implementation.
 *
 * @param <IN>  The type of input tuples.
 * @param <OUT> The type of output tuples.
 */
public class TimeMWAggregate<IN extends RichTuple, OUT extends RichTuple>
        extends TimeAggregate<IN, OUT> {

//    protected final long WS_WA_ceil;
    protected TimeWindowAdd<IN, OUT> aggregateWindow;
    //    protected long WS_WA_ceil_minus_1;
    TreeMap<Long, HashMap<String, TimeWindowAdd<IN, OUT>>> windows;

    public TimeMWAggregate(
            String id,
            int instance,
            int parallelismDegree,
            long windowSize,
            long windowSlide,
            TimeWindowAdd<IN, OUT> aggregateWindow) {
        super(id, instance, parallelismDegree, windowSize, windowSlide, aggregateWindow);
        TimeMWAggregate.this.windows = new TreeMap<>();
        this.aggregateWindow = aggregateWindow;
//        this.WS_WA_ceil = (long) Math.ceil((double) TimeBasedMultiWindowAggregate.this.WS / (double) TimeBasedMultiWindowAggregate.this.WA);
//        this.WS_WA_ceil_minus_1 = TimeBasedMultiWindowAggregate.this.WS_WA_ceil - 1;
    }

    public List<OUT> processTupleIn1(IN t) {

        List<OUT> result = new LinkedList<OUT>();

        checkIncreasingTimestamps(t);

        latestTimestamp = t.getTimestamp();

        long earliestWinStartTSforT = getEarliestWinStartTS(latestTimestamp);

        // Managing of stale windows
        boolean purgingNotDone = true;
        while (purgingNotDone && windows.size() > 0) {

            long earliestWinStartTS = windows.firstKey();

            if (earliestWinStartTS + WS <= latestTimestamp) {

                // Produce results for stale windows
                for (TimeWindowAdd<IN, OUT> w : windows.get(earliestWinStartTS).values()) {
                    OUT outT = w.getAggregatedResult();
                    if (outT!=null) {
                        result.add(outT);
                    }
                }

                // Remove stale windows
                windows.remove(earliestWinStartTS);

            } else {
                purgingNotDone = false;
            }

        }

        // Add this tuple to all the windows it contributes to
        long timestamp = earliestWinStartTSforT;
        while (timestamp <= latestTimestamp) {

            // Add contribution of this tuple
            if (!windows.containsKey(timestamp)) {
                windows.put(timestamp, new HashMap<>());
            }
            if (!windows.get(timestamp).containsKey(t.getKey())) {
                windows.get(timestamp).put(t.getKey(), aggregateWindow.factory());
                windows.get(timestamp).get(t.getKey()).setKey(t.getKey());
                windows.get(timestamp).get(t.getKey()).setStartTimestamp(timestamp);
            }

            windows.get(timestamp).get(t.getKey()).add(t);

            timestamp += WA;
        }

        return result;
    }

}
