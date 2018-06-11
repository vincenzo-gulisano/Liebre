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

import common.StreamConsumer;
import common.StreamProducer;
import common.component.ConnectionsNumber;
import common.component.EventType;
import common.tuple.Tuple;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import scheduling.priority.PriorityMetric;
import stream.Stream;

public class RouterOperatorDecorator<T extends Tuple> implements RouterOperator<T> {

  private final RouterOperator<T> decorated;
  private final ProcessCommandRouter<T> processingCommand = new ProcessCommandRouter<>(this);

  public RouterOperatorDecorator(RouterOperator<T> decorated) {
    this.decorated = decorated;
  }

  @Override
  public String getId() {
    return decorated.getId();
  }

  @Override
  public int getIndex() {
    return decorated.getIndex();
  }

  @Override
  public Collection<? extends Stream<T>> getOutputs() {
    return decorated.getOutputs();
  }

  @Override
  public Collection<? extends Stream<T>> getInputs() {
    return decorated.getInputs();
  }

  @Override
  public boolean canRead() {
    return decorated.canRead();
  }

  @Override
  public void addInput(StreamProducer<T> source, Stream<T> stream) {
    decorated.addInput(source, stream);
  }

  @Override
  public Stream<T> getInput() {
    return decorated.getInput();
  }

  @Override
  public boolean canWrite() {
    return decorated.canWrite();
  }

  @Override
  public void waitFor(EventType type) {
    decorated.waitFor(type);
  }

  @Override
  public void notifyFor(EventType type) {
    decorated.notifyFor(type);
  }

  @Override
  public void addOutput(StreamConsumer<T> destination, Stream<T> stream) {
    decorated.addOutput(destination, stream);
  }

  @Override
  public Stream<T> getOutput() {
    return decorated.getOutput();
  }

  @Override
  public void run() {
    processingCommand.run();
  }

  @Override
  public List<String> chooseOperators(T tuple) {
    return decorated.chooseOperators(tuple);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    RouterOperatorDecorator<?> that = (RouterOperatorDecorator<?>) o;

    return new EqualsBuilder()
        .append(decorated, that.decorated)
        .isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37)
        .append(decorated)
        .toHashCode();
  }

  @Override
  public String toString() {
    return decorated.toString();
  }

  @Override
  public void enable() {
    decorated.enable();
  }

  @Override
  public boolean isEnabled() {
    return decorated.isEnabled();
  }

  @Override
  public void disable() {
    decorated.disable();
  }

  @Override
  public void onScheduled() {
    decorated.onScheduled();
  }

  @Override
  public void onRun() {
    decorated.onRun();
  }

  @Override
  public ConnectionsNumber inputsNumber() {
    return decorated.inputsNumber();
  }

  @Override
  public ConnectionsNumber outputsNumber() {
    return decorated.outputsNumber();
  }

  public void setPriorityMetric(PriorityMetric metric) {
    decorated.setPriorityMetric(metric);
  }

}
