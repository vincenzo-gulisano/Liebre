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

package operator.map;

import java.util.LinkedList;
import java.util.List;

import common.tuple.Tuple;
import operator.in1.BaseOperator1In;
import org.apache.commons.lang3.Validate;

/**
 * {@link operator.Operator} that applies a given {@link MapFunction} to each tuple of its input
 * stream.
 *
 * @param <IN> The type of input tuples.
 * @param <OUT> The type of output tuples.
 */
public class MapOperator<IN extends Tuple, OUT extends Tuple> extends BaseOperator1In<IN, OUT> {

  private MapFunction<IN, OUT> map;

  /**
   * Construct.
   *
   * @param id The unique id of this operator.
   * @param map The {@link MapFunction} to be applied to every input tuple.
   */
  public MapOperator(String id, MapFunction<IN, OUT> map) {
    super(id);
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
    List<OUT> result = new LinkedList<OUT>();
    OUT t = map.apply(tuple);
    if (t != null) {
      result.add(t);
    }
    return result;
  }

}
