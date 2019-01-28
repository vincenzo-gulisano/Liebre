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

package component.operator.in1;

import component.ComponentType;
import common.tuple.Tuple;
import component.operator.AbstractOperator;

/**
 * Default abstract implementation of {@link Operator1In}.
 *
 * @param <IN> The type of input tuples.
 * @param <OUT> The type of output tuples.
 *
 * @author palivosd
 */
public abstract class BaseOperator1In<IN extends Tuple, OUT extends Tuple> extends
    AbstractOperator<IN, OUT>
    implements Operator1In<IN, OUT> {

  private final ProcessCommand1In<IN, OUT> processCommand = new ProcessCommand1In<>(this);

  /**
   * Construct.
   *
   * @param id The unique id of this component.
   */
  public BaseOperator1In(String id) {
    super(id, ComponentType.OPERATOR);
  }

  @Override
  public void run() {
    processCommand.run();
  }

}
