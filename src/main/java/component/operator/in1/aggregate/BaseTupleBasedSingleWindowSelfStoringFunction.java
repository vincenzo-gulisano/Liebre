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

/**
 * Default implementation of {@link TupleBasedSingleWindowSelfStoringFunction}, maintaining the trivial state
 * (instanceNumber and parallelismDegree)
 */
public abstract class BaseTupleBasedSingleWindowSelfStoringFunction<IN, OUT>
        implements TupleBasedSingleWindowSelfStoringFunction<IN, OUT> {

    protected int instanceNumber;
    protected int parallelismDegree;

    @Override
    public void setInstanceNumber(int aggregateInstanceNumber) {
        this.instanceNumber = aggregateInstanceNumber;
    }

    @Override
    public abstract TupleBasedSingleWindowSelfStoringFunction<IN, OUT> factory();

    @Override
    public abstract void add(IN t);

    @Override
    public abstract void slideBy(int tuples);

    @Override
    public abstract OUT getAggregatedResult();

    @Override
    public void setParallelismDegree(int parallelismDegree) {
        this.parallelismDegree = parallelismDegree;
    }
}
