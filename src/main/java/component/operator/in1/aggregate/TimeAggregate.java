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

import java.util.*;

/**
 * Aggregate implementation for sliding time-based windows. Decides which tuples belong to which
 * windows and takes care of producing aggregation results by delegating to a provided {@link
 * TimeWindowAddRemove} implementation.
 *
 * @param <IN>  The type of input tuples.
 * @param <OUT> The type of output tuples.
 */
public class TimeAggregate<IN extends RichTuple, OUT extends RichTuple>
        extends BaseOperator1In<IN, OUT> {

    private final int instance;
    private final int parallelismDegree;
    private final long WS;
    private final long WA;
    private TimeWindowAddSlide<IN, OUT> aggregateWindow;
    private long latestTimestamp;
    private boolean firstTuple = true;
    private TreeMap<Long, HashMap<String, TimeWindowAddSlide<IN, OUT>>> windows;

    public TimeAggregate(
            String id,
            int instance,
            int parallelismDegree,
            long windowSize,
            long windowSlide,
            TimeWindowAddSlide<IN, OUT> aggregateWindow) {
        super(id);
        this.instance=instance;
        this.parallelismDegree=parallelismDegree;
        windows = new TreeMap<>();
        this.WS = windowSize;
        this.WA = windowSlide;
        this.aggregateWindow = aggregateWindow;
    }

    public TimeAggregate(
            String id,
            int instance,
            int parallelismDegree,
            long windowSize,
            long windowSlide,
            TimeWindowAddRemove<IN, OUT> aggregateWindow) {
        super(id);
        this.instance=instance;
        this.parallelismDegree=parallelismDegree;
        windows = new TreeMap<>();
        this.WS = windowSize;
        this.WA = windowSlide;
        this.aggregateWindow = new TimeWindowAddRemoveWrapper<>(aggregateWindow);
    }

    public List<OUT> processTupleIn1(IN t) {

        List<OUT> result = new LinkedList<OUT>();

        checkIncreasingTimestamps(t);

        latestTimestamp = t.getTimestamp();

        long earliestWinStartTSforT = getEarliestWinStartTS(latestTimestamp);

        // Keep track of windows that became empty
        Set<String> emptyWins = new HashSet<>();

        // Managing of stale windows
        boolean purgingNotDone = true;
        while (purgingNotDone && windows.size() > 0) {

            long earliestWinStartTS = windows.firstKey();

            if (earliestWinStartTS + WS <= latestTimestamp) {

                // Produce results for stale windows
                for (TimeWindowAddSlide<IN, OUT> w : windows.get(earliestWinStartTS).values()) {
                    OUT outT = w.getAggregatedResult();
                    if (outT!=null) {
                        result.add(outT);
                    }
                }

                // Shift windows
                if (!windows.containsKey(earliestWinStartTS + WA)) {
                    windows.put(
                            earliestWinStartTS + WA, new HashMap<>());
                }
                windows.get(earliestWinStartTS + WA).putAll(windows.get(earliestWinStartTS));
                for (String s : windows.get(earliestWinStartTS + WA).keySet()) {
                    windows.get(earliestWinStartTS + WA).get(s).slideTo(earliestWinStartTS + WA);
                    if (windows.get(earliestWinStartTS + WA).get(s).isEmpty()) {
                        emptyWins.add(s);
                    }
                }
                windows.remove(earliestWinStartTS);
            } else {
                purgingNotDone = false;
            }
        }

        // Remove empty windows
        for(String s : emptyWins) {
            windows.firstEntry().getValue().remove(s);
        }

        // Add contribution of this tuple
        if (!windows.containsKey(earliestWinStartTSforT)) {
            windows.put(earliestWinStartTSforT, new HashMap<>());
        }
        if (!windows.get(earliestWinStartTSforT).containsKey(t.getKey())) {
            windows.get(earliestWinStartTSforT).put(t.getKey(), aggregateWindow.factory());
            windows.get(earliestWinStartTSforT).get(t.getKey()).setKey(t.getKey());
            windows.get(earliestWinStartTSforT).get(t.getKey()).setInstanceNumber(instance);
            windows.get(earliestWinStartTSforT).get(t.getKey()).setParallelismDegree(parallelismDegree);
            windows.get(earliestWinStartTSforT).get(t.getKey()).slideTo(earliestWinStartTSforT);
        }

        windows.get(earliestWinStartTSforT).get(t.getKey()).add(t);

        return result;
    }

    public long getEarliestWinStartTS(long ts) {
        long winStart = (ts / WA) * WA;
        while (winStart - WA + WS > ts) {
            winStart -= WA;
        }
        return Math.max(0, winStart);
//        long contributingWins = (ts % WA < WS % WA || WS % WA == 0) ? WS_WA_ceil : WS_WA_ceil_minus_1;
//        return (long) Math.max((ts / WA - contributingWins + 1) * WA, 0.0);
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

    protected void checkIncreasingTimestamps(IN t) {
        if (firstTuple) {
            firstTuple = false;
        } else {
            if (t.getTimestamp() < latestTimestamp) {
                throw new RuntimeException("Input tuple's timestamp decreased!");
            }
        }
    }
}
