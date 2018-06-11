/*
 * Copyright (C) 2017-2018
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

package operator.router;

import java.util.List;

import common.tuple.Tuple;
import java.util.Objects;
import operator.AbstractProcessCommand;
import stream.Stream;

public class ProcessCommandRouter<T extends Tuple> extends
    AbstractProcessCommand<RouterOperator<T>> {

  protected ProcessCommandRouter(RouterOperator<T> operator) {
    super(operator);
  }

  @Override
  public final void process() {
    Stream<T> input = operator.getInput();
    T inTuple = input.getNextTuple();
    if (inTuple != null) {
      metric.recordTupleRead(inTuple, input);
      List<String> streams = operator.chooseOperators(inTuple);
      if (streams != null) {
        for (String op : streams) {
          Stream<T> output = getOutputStream(op);
          metric.recordTupleWrite(inTuple, output);
          output.addTuple(inTuple);
        }
      }
    }
  }

  private Stream<T> getOutputStream(String id) {
    for (Stream<T> stream : operator.getOutputs()) {
      if (Objects.equals(stream.getId(), id)) {
        return stream;
      }
    }
    throw new IllegalStateException(String.format(
        "Requested output stream with id '%s' but operator '%s' has the following outputs: %s", id,
        operator.getId(), operator.getOutputs()));
  }
}
