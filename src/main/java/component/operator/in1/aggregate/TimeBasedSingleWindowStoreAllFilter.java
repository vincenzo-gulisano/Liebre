package component.operator.in1.aggregate;

import common.tuple.RichTuple;

public class TimeBasedSingleWindowStoreAllFilter<T extends RichTuple> implements TimeBasedSingleWindowStoringFilter<T> {
    @Override
    public boolean keep(T t, int instanceNumber) {
        return true;
    }
}
