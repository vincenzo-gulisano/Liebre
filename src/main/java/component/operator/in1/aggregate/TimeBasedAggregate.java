package component.operator.in1.aggregate;

import common.tuple.RichTuple;
import component.operator.in1.BaseOperator1In;

import java.util.HashMap;
import java.util.TreeMap;

public abstract class TimeBasedAggregate<IN extends RichTuple, OUT extends RichTuple> extends BaseOperator1In<IN, OUT> {
    protected final long WS_WA_ceil;
    protected long WS;
    protected long WA;
    protected TimeWindowAddRemove<IN, OUT> aggregateWindow;
    protected long latestTimestamp;
    protected boolean firstTuple = true;
    protected long WS_WA_ceil_minus_1;
    TreeMap<Long, HashMap<String, TimeWindowAddRemove<IN, OUT>>> windows;

    public TimeBasedAggregate(String id, long windowSize, long windowSlide, TimeWindowAddRemove<IN, OUT> aggregateWindow) {
        super(id);
        windows = new TreeMap<>();
        this.WS = windowSize;
        this.WA = windowSlide;
        this.aggregateWindow = aggregateWindow;
        WS_WA_ceil = (long) Math.ceil((double) WS / (double) WA);
        WS_WA_ceil_minus_1 = WS_WA_ceil - 1;
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
