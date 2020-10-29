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
 * Default implementation of {@link TimeBasedSingleWindow}, maintaining the trivial state, including
 * the {@code key} of the tuples and the {@code timestamp} of the earliest tuple of this window.
 */
public abstract class BaseTimeBasedSingleWindowSelfStoringFunction<IN extends RichTuple, OUT extends RichTuple>
        implements TimeBasedSingleWindowSelfStoringFunction<IN, OUT> {

    protected String key;
    protected int instanceNumber;
    protected int parallelismDegree;

    @Override
    public void setInstanceNumber(int aggregateInstanceNumber) {
        this.instanceNumber = aggregateInstanceNumber;
    }

    @Override
    public abstract void add(IN t);

    @Override
    public abstract void slideTo(long startTimestamp);

    @Override
    public abstract OUT getAggregatedResult();

    @Override
    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public void setParallelismDegree(int parallelismDegree) {
        this.parallelismDegree = parallelismDegree;
    }
}
