package component.operator.in1.aggregate;

import common.tuple.RichTuple;

public interface KeyExtractor<T extends RichTuple> {

    String getKey(T t);

}
