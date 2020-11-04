package component.operator.in1.aggregate;

import common.tuple.RichTuple;

public interface TimeWindow<IN extends RichTuple, OUT extends RichTuple> {

    /**
     * Called when a new tuple is added to the window. The state of the window can be updated.
     *
     * @param t The new tuple that is added to the window.
     */
    void add(IN t);

    /**
     * Called when a window must produce a result based on its current state, i.e., the tuples
     * currently present in it.
     *
     * @return The aggregation result.
     */
    OUT getAggregatedResult();

    /**
     * Setter for the key of the tuples that belong to this window.
     *
     * @param key The key of the tuples that belong to this window.
     */
    void setKey(String key);

    /**
     * Setter for the instance number of the aggregate maintaining this window.
     *
     * @param instanceNumber The instance number of the aggregate maintaining this window.
     */
    void setInstanceNumber(int instanceNumber);

    /**
     * Setter for the parallelismDegree number of the aggregate maintaining this window.
     *
     * @param parallelismDegree The parallelism degree of the aggregate maintaining this window.
     */
    void setParallelismDegree(int parallelismDegree);

}
