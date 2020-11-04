package component.operator.in1.aggregate;

import common.tuple.RichTuple;
import component.ComponentFunction;

public interface TimeWindow<IN extends RichTuple, OUT extends RichTuple> extends Window<IN, OUT> {

    /**
     * Setter for the key of the tuples that belong to this window.
     *
     * @param key The key of the tuples that belong to this window.
     */
    void setKey(String key);

}
