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

package operator.router;

import common.StreamConsumer;
import common.component.ComponentType;
import common.tuple.Tuple;
import java.util.Collection;
import operator.AbstractOperator;
import scheduling.priority.PriorityMetric;
import stream.Stream;
import stream.StreamFactory;

public class BaseRouterOperator<T extends Tuple> extends AbstractOperator<T, T> implements
    RouterOperator<T> {

  private static final int INPUT_KEY = 0;
  private final ProcessCommandRouter<T> processCommand = new ProcessCommandRouter<>(this);

  public BaseRouterOperator(String id, StreamFactory streamFactory) {
    super(id, ComponentType.ROUTER);
  }

  @Override
  public Collection<? extends Stream<T>> chooseOutputs(T tuple) {
    return getOutputs();
  }

  @Override
  public void addOutput(StreamConsumer<T> destination, Stream<T> stream) {
    state.addOutput(stream);
  }

  public Stream<T> getOutput() {
    throw new UnsupportedOperationException(
        String.format("'%s': Router has multiple outputs!", state.getId()));
  }

  @Override
  public void run() {
    processCommand.run();
  }

  @Override
  public void setPriorityMetric(PriorityMetric metric) {
    processCommand.setMetric(metric);
  }

  @Override
  public void enable() {
    super.enable();
  }

  @Override
  public void disable() {
    super.disable();
  }
}
