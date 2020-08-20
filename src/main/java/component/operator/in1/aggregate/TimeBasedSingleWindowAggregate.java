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
import component.operator.in1.BaseOperator1In;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

/**
 * Aggregate implementation for sliding time-based windows. Decides which tuples belong to which
 * windows and takes care of producing aggregation results by delegating to a provided {@link
 * TimeBasedSingleWindow} implementation.
 *
 * @param <IN>  The type of input tuples.
 * @param <OUT> The type of output tuples.
 */
public class TimeBasedSingleWindowAggregate<IN extends RichTuple, OUT extends RichTuple>
        extends BaseOperator1In<IN, OUT> {

    LinkedList<IN> tuples;
    TreeMap<Long, HashMap<String, TimeBasedSingleWindow<IN, OUT>>> windows;
    TreeMap<Long, HashMap<String, WinCounter>> windowsCounters;
    private long WS;
    private long WA;
    private TimeBasedSingleWindow<IN, OUT> aggregateWindow;
    private long latestTimestamp;
    private boolean firstTuple = true;

    private final long WS_WA_ceil;
    private long WS_WA_ceil_minus_1;

    public TimeBasedSingleWindowAggregate(
            String id,
            long windowSize,
            long windowSlide,
            TimeBasedSingleWindow<IN, OUT> aggregateWindow) {
        super(id);
        tuples = new LinkedList<>();
        windows = new TreeMap<>();
        windowsCounters = new TreeMap<>();
        this.WS = windowSize;
        this.WA = windowSlide;
        this.aggregateWindow = aggregateWindow;

        // this is for speeding up how to find the windows to which a tuple contributes to.
        WS_WA_ceil = (long) Math.ceil((double)WS / (double)WA);
        WS_WA_ceil_minus_1 = WS_WA_ceil-1;
    }

    public long getEarliestWinStartTS(long ts) {
        long contributingWins = ts % WA < WS % WA ? WS_WA_ceil : WS_WA_ceil_minus_1;
        return (long) Math.max((ts / WA - contributingWins + 1) * WA, 0.0);
    }

    public List<OUT> processTupleIn1(IN t) {

        List<OUT> result = new LinkedList<OUT>();

        // make sure timestamps are not decreasing
        if (firstTuple) {
            firstTuple = false;
        } else {
            if (t.getTimestamp() < latestTimestamp) {
                throw new RuntimeException("Input tuple's timestamp decreased!");
            }
        }
        latestTimestamp = t.getTimestamp();

        long earliestWinStartTSforT = getEarliestWinStartTS(latestTimestamp);

        // Managing of stale windows
        boolean purgingNotDone = true;
        while (purgingNotDone && windows.size() > 0) {

            long earliestWinStartTS = windows.firstKey();

            if (earliestWinStartTS + WS <= latestTimestamp) {

                // Produce results for stale windows
                for (TimeBasedSingleWindow<IN, OUT> w : windows.get(earliestWinStartTS).values()) {
                    result.add(w.getAggregatedResult());
                }

                // Remove contribution of stale tuples from stale windows
                while (tuples.size() > 0) {
                    IN tuple = tuples.peek();
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
                    windows.put(
                            earliestWinStartTS + WA, new HashMap<>());
                    windowsCounters.put(earliestWinStartTS + WA, new HashMap<>());
                }
                windows.get(earliestWinStartTS + WA).putAll(windows.get(earliestWinStartTS));
                windowsCounters
                        .get(earliestWinStartTS + WA)
                        .putAll(windowsCounters.get(earliestWinStartTS));
                for (TimeBasedSingleWindow<IN, OUT> w : windows.get(earliestWinStartTS + WA).values()) {
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
            windows.put(earliestWinStartTSforT, new HashMap<>());
            windowsCounters.put(earliestWinStartTSforT, new HashMap<>());
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

    @Override
    public void enable() {
        aggregateWindow.enable();
        super.enable();
    }

    @Override
    public void disable() {
        super.disable();
        aggregateWindow.disable();
    }

    @Override
    public boolean canRun() {
        return aggregateWindow.canRun() && super.canRun();
    }

    private static class WinCounter {

        private long count = 0;

        public void add(long v) {
            count += v;
        }

        public boolean isZero() {
            return count == 0;
        }
    }
}
