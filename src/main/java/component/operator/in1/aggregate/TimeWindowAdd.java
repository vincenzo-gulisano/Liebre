package component.operator.in1.aggregate;

import common.tuple.RichTuple;
import component.ComponentFunction;

public interface TimeWindowAdd<IN extends RichTuple, OUT extends RichTuple> extends
        ComponentFunction, TimeWindow<IN, OUT> {

    /**
     * Generate a new {@link TimeWindowAddSlide} with the same configuration and probably a clear
     * state.
     *
     * @return A new {@link TimeWindowAddSlide} instance.
     */
    TimeWindowAdd<IN, OUT> factory();

    /**
     * Setter for the timestamp of the earliest tuple in this window.
     *
     * @param startTimestamp The timestamp of the earliest tuple in the window.
     */
    public void setStartTimestamp(long startTimestamp);

}
