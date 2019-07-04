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
import component.ComponentState;
import component.ComponentType;
import component.ConnectionsNumber;
import common.tuple.Tuple;

import java.util.Collection;

import stream.Stream;


public abstract class AbstractSource<OUT extends Tuple> implements Source<OUT> {

  private static final int OUTPUT_KEY = 0;
  protected final ComponentState<Tuple, OUT> state;
  private final ProcessCommandSource<OUT> processCommand = new ProcessCommandSource<>(this);

  private int priority;
  private int relativeProducerIndex;

  public AbstractSource(String id,int relativeProducerIndex) {
    this.state = new ComponentState<>(id, ComponentType.SOURCE);
    this.relativeProducerIndex=relativeProducerIndex;
  }

  @Override
  public void addOutput(StreamConsumer<OUT> destination, Stream<OUT> stream) {
    state.addOutput(OUTPUT_KEY, stream);
  }

  @Override
  public ComponentType getType() {
    return state.getType();
  }

  @Override
  public Stream<OUT> getOutput() {
    return state.getOutput(OUTPUT_KEY);
  }

  @Override
  public Collection<? extends Stream<OUT>> getOutputs() {
    return state.getOutputs();
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
  public boolean runFor(int times) {
    return processCommand.runFor(times);
  }

  @Override
  public boolean canRun() {
    return getOutput().remainingCapacity() > 0;
  }

  @Override
  public void run() {
    processCommand.run();
  }

  @Override
  public ConnectionsNumber inputsNumber() {
    return state.inputsNumber();
  }

  @Override
  public ConnectionsNumber outputsNumber() {
    return state.outputsNumber();
  }

  @Override
  public void enable() {
    state.enable();
  }

  @Override
  public void disable() {
    state.disable();
  }

  @Override
  public boolean isEnabled() {
    return state.isEnabled();
  }

  @Override
  public abstract OUT getNextTuple();

  @Override
  public String getId() {
    return state.getId();
  }

  public int getPriority() {
    return priority;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }

  @Override
  public int getIndex() {
    return state.getIndex();
  }

  @Override
  public String toString() {
    return getId();
  }
  
  @Override
	public int getRelativeProducerIndex(int index) {
		return relativeProducerIndex;
	}
  
  @Override
	public int getRelativeConsumerIndex(int index) {
		throw new UnsupportedOperationException("Sources are not consumers!");
	}
}
