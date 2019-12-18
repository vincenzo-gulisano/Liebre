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

package component.operator.router;

import component.AbstractProcessCommand;
import component.ProcessCommand;
import stream.Stream;

/**
 * {@link ProcessCommand} implementation for {@link BaseRouterOperator}.
 *
 * @param <T> The type of input/output tuples.
 */
class ProcessCommandRouter<T> extends
    AbstractProcessCommand<RouterOperator<T>> {

  protected ProcessCommandRouter(RouterOperator<T> operator) {
    super(operator);
  }

  @Override
  public final void process() {
    Stream<T> input = component.getInput();
    T inTuple = input.getNextTuple(component.getRelativeConsumerIndex());
    if (inTuple != null) {
      increaseTuplesRead();
      for (Stream<T> output : component.chooseOutputs(inTuple)) {
        increaseTuplesWritten();
        output.addTuple(inTuple,component.getRelativeProducerIndex());
      }
    }
  }

}
