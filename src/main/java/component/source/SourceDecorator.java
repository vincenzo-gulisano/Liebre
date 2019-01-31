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

package component.source;

import component.StreamConsumer;
import component.ConnectionsNumber;
import common.tuple.Tuple;
import java.util.Collection;
import stream.Stream;

/**
 * Base decorator for {@link Source}. Delegates all function calls to the decorated object.
 *
 * @param <OUT> The type of output tuples.
 * @author palivosd
 */
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
  public Collection<? extends Stream<OUT>> getOutputs() {
    return decorated.getOutputs();
  }

  @Override
  public void addOutput(StreamConsumer<OUT> destination, Stream<OUT> stream) {
    decorated.addOutput(destination, stream);
  }

  @Override
  public void runFor(int times) {
    processCommand.runFor(times);
  }

  @Override
  public boolean canRun() {
    return decorated.canRun();
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
  public ConnectionsNumber inputsNumber() {
    return decorated.inputsNumber();
  }

  @Override
  public ConnectionsNumber outputsNumber() {
    return decorated.outputsNumber();
  }

  @Override
  public String toString() {
    return decorated.toString();
  }

}
