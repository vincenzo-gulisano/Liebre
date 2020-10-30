/*
 * Copyright (C) 2017-2019
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package component.operator.in1.aggregate;

import common.tuple.RichTuple;
import component.operator.in1.BaseOperator1In;

import java.util.*;

/**
 * Aggregate implementation for sliding tuple-based windows. This Aggregate does not check whether the input stream
 * is in timestamp-order. That's because tuple-based windows behavior does not depend on event time.
 *
 * @param <IN>  The type of input tuples.
 * @param <OUT> The type of output tuples.
 */
public class TupleBasedSingleWindowSelfStoringAggregate<IN, OUT>
        extends BaseOperator1In<IN, OUT> {

    private final int instance;
    private final int parallelismDegree;
    private final long WS;
    private final long WA;
    private TupleBasedSingleWindowSelfStoringFunction<IN, OUT> aggregateWindow;
    private boolean firstTuple = true;
    private long tuples;

    public TupleBasedSingleWindowSelfStoringAggregate(
            String id,
            int instance,
            int parallelismDegree,
            long windowSize,
            long windowSlide,
            TupleBasedSingleWindowSelfStoringFunction<IN, OUT> aggregateWindow) {
        super(id);
        this.instance=instance;
        this.parallelismDegree=parallelismDegree;
        this.WS = windowSize;
        this.WA = windowSlide;
        this.aggregateWindow = aggregateWindow;
        this.aggregateWindow.setInstanceNumber(this.instance);
        this.aggregateWindow.setParallelismDegree(this.parallelismDegree);
        assert(WA<=WS);
    }

    public List<OUT> processTupleIn1(IN t) {

        List<OUT> result = new LinkedList<OUT>();

        aggregateWindow.add(t);
        tuples++;

        if (tuples==WS) {
            OUT outT = aggregateWindow.getAggregatedResult();
            if (outT!=null) {
                result.add(outT);
            }
            aggregateWindow.slideBy(WA);
            tuples-=WA;
        }

        return result;
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

}
