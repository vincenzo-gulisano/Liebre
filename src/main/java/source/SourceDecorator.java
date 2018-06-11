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

package source;

import common.StreamConsumer;
import common.component.ConnectionsNumber;
import common.component.EventType;
import common.tuple.Tuple;
import java.util.Collection;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import scheduling.priority.PriorityMetric;
import stream.Stream;

public class SourceDecorator<OUT extends Tuple> implements Source<OUT> {

  private final Source<OUT> decorated;
  private final ProcessCommandSource<OUT> processCommand = new ProcessCommandSource<>(this);

  public SourceDecorator(Source<OUT> decorated) {
    this.decorated = decorated;
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
  public void run() {
    processCommand.run();
  }

  @Override
  public boolean canWrite() {
    return decorated.canWrite();
  }

  @Override
  public void wait(EventType type) {
    decorated.wait(type);
  }

  @Override
  public void notify(EventType type) {
    decorated.notify(type);
  }

  @Override
  public Collection<? extends Stream<OUT>> getOutputs() {
    return decorated.getOutputs();
  }

  @Override
  public void addOutput(StreamConsumer<OUT> destination, Stream<OUT> stream) {
    decorated.addOutput(destination, stream);
  }

  @Override
  public Stream<OUT> getOutput() {
    return decorated.getOutput();
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
  public OUT getNextTuple() {
    return decorated.getNextTuple();
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

  @Override
  public void setPriorityMetric(PriorityMetric metric) {
    decorated.setPriorityMetric(metric);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SourceDecorator<?> that = (SourceDecorator<?>) o;

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

}
