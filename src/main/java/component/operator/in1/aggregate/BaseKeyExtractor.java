package component.operator.in1.aggregate;

import common.tuple.RichTuple;

public class BaseKeyExtractor<T extends RichTuple> implements KeyExtractor<T> {

    @Override
    public String getKey(T t) {
        return t.getKey();
    }

}
