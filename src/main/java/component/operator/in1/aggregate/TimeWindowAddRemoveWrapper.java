package component.operator.in1.aggregate;

import common.tuple.RichTuple;

import java.util.LinkedList;

public class TimeWindowAddRemoveWrapper<IN extends RichTuple, OUT extends RichTuple> implements TimeWindowAddSlide<IN,OUT> {

    private TimeWindowAddRemove<IN,OUT> w;
    LinkedList<IN> tuples;

    public TimeWindowAddRemoveWrapper(TimeWindowAddRemove<IN,OUT> w) {
        this.w = w;
        tuples = new LinkedList<>();
    }

    @Override
    public TimeWindowAddSlide factory() {
        return new TimeWindowAddRemoveWrapper(w.factory());
    }

    @Override
    public void add(IN t) {
        w.add(t);
        tuples.add(t);
    }

    @Override
    public void slideTo(long startTimestamp) {
        while (tuples.size() > 0) {
            IN tuple = tuples.peek();
            if (tuple.getTimestamp() < startTimestamp) {
                w.remove(tuple);
                tuples.pop();
            } else {
                break;
            }
        }
        w.setStartTimestamp(startTimestamp);
    }

    @Override
    public OUT getAggregatedResult() {
        return w.getAggregatedResult();
    }

    @Override
    public void setKey(String key) {
        w.setKey(key);
    }

    @Override
    public void setInstanceNumber(int instanceNumber) {
        w.setInstanceNumber(instanceNumber);
    }

    @Override
    public void setParallelismDegree(int parallelismDegree) {
        w.setParallelismDegree(parallelismDegree);
    }

    @Override
    public boolean isEmpty() {
        return tuples.size()==0;
    }
}
