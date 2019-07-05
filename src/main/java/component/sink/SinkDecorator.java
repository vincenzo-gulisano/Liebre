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

package component.sink;

import component.ComponentType;
import component.StreamProducer;
import component.ConnectionsNumber;
import common.tuple.Tuple;
import java.util.Collection;
import stream.Stream;

/**
 * Base decorator for {@link Sink}. Delegates all function calls to the decorated object.
 *
 * @param <IN> The type of input tuples.
 * @author palivosd
 */
public class SinkDecorator<IN extends Tuple> implements Sink<IN> {

  private final Sink<IN> decorated;
  private final ProcessCommandSink<IN> processCommand = new ProcessCommandSink<>(this);

  public SinkDecorator(Sink<IN> decorated) {
    this.decorated = decorated;
  }

  @Override
  public ComponentType getType() {
    return decorated.getType();
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
  public void addInput(StreamProducer<IN> source, Stream<IN> stream) {
    decorated.addInput(source, stream);
  }

  @Override
  public Stream<IN> getInput() {
    return decorated.getInput();
  }

  @Override
  public Collection<? extends Stream<IN>> getInputs() {
    return decorated.getInputs();
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
  public void processTuple(IN tuple) {
    decorated.processTuple(tuple);
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
  public void run() {
    processCommand.run();
  }

  @Override
  public boolean runFor(int times) {
    return processCommand.runFor(times);
  }

  @Override
  public void updateMetrics() {
    processCommand.updateMetrics();
  }

  @Override
  public double getSelectivity() {
    return processCommand.getSelectivity();
  }

  @Override
  public double getCost() {
    return processCommand.getCost();
  }

  @Override
  public double getRate() {
    return processCommand.getRate();
  }

  @Override
  public boolean canRun() {
    return decorated.canRun();
  }

  @Override
  public String toString() {
    return decorated.toString();
  }
  
  @Override
	public int getRelativeProducerIndex() {
		return decorated.getRelativeProducerIndex();
	}
  

  @Override
	public int getRelativeConsumerIndex() {
		return decorated.getRelativeConsumerIndex();
	}
}
