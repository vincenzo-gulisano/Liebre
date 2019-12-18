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

package component.operator.in1.map;

import component.operator.in1.BaseOperator1In;
import java.util.List;
import org.apache.commons.lang3.Validate;

/**
 * {@link component.operator.Operator} that applies a given {@link FlatMapFunction} to the tuples of a
 * stream.
 *
 * @param <IN> The type of input tuples.
 * @param <OUT> The type of output tuples.
 */
public class FlatMapOperator<IN, OUT> extends BaseOperator1In<IN, OUT> {

  private FlatMapFunction<IN, OUT> map;

  /**
   * Construct.
   *
   * @param id The unique id of this component.operator.
   * @param map The {@link FlatMapFunction} that will be applied to each input tuple.
   */
	public FlatMapOperator(String id, int relativeProducerIndex,
			int relativeConsumerIndex, FlatMapFunction<IN, OUT> map) {
		super(id, relativeProducerIndex, relativeConsumerIndex);
		Validate.notNull(map, "map");
		this.map = map;
	}

  @Override
  public void enable() {
    super.enable();
    map.enable();
  }

  @Override
  public void disable() {
    map.disable();
    super.disable();
  }

  @Override
  public List<OUT> processTupleIn1(IN tuple) {
    return map.apply(tuple);
  }
}
