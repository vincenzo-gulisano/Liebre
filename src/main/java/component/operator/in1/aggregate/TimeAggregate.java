package component.operator.in1.aggregate;

import common.tuple.RichTuple;
import component.operator.in1.BaseOperator1In;

public abstract class TimeAggregate<IN extends RichTuple, OUT extends RichTuple> extends BaseOperator1In<IN, OUT> {
    protected final int instance;
    protected final int parallelismDegree;
    protected final long WS;
    protected final long WA;
    protected final TimeWindow w;
    private final long WS_WA_ceil;
    private final long WS_WA_ceil_minus_1;
    protected long latestTimestamp;
    protected KeyExtractor keyExtractor;
    private boolean firstTuple = true;

    public TimeAggregate(String id, int instance, int parallelismDegree, long ws, long wa, TimeWindow w, KeyExtractor keyExtractor) {
        super(id);
        this.instance = instance;
        this.parallelismDegree = parallelismDegree;
        WS = ws;
        WA = wa;
        this.w = w;
        this.keyExtractor = keyExtractor;

        if ((wa > ws)) throw new AssertionError();
        WS_WA_ceil = (long) Math.ceil((double) WS / (double) WA);
        WS_WA_ceil_minus_1 = WS_WA_ceil - 1;
    }

    public long getContributingWindows(long ts) {
        return (ts % WA < WS % WA || WS % WA == 0) ? WS_WA_ceil : WS_WA_ceil_minus_1;
    }

    public long getEarliestWinStartTS(long ts) {
        return (long) Math.max((ts / WA - getContributingWindows(ts) + 1) * WA, 0.0);
    }

    @Override
    public void enable() {
        w.enable();
        super.enable();
    }

    @Override
    public void disable() {
        super.disable();
        w.disable();
    }

    @Override
    public boolean canRun() {
        return w.canRun() && super.canRun();
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

    public void registerKeyExtractor(KeyExtractor k) {
        this.keyExtractor = k;
    }
}
