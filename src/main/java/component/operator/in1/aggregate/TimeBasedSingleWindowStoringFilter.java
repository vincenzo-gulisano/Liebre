package component.operator.in1.aggregate;

import common.tuple.RichTuple;

public interface TimeBasedSingleWindowStoringFilter<T extends RichTuple> {

    public boolean keep(T t,int instanceNumber);

}
