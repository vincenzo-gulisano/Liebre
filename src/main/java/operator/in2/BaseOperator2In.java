/*  Copyright (C) 2017  Vincenzo Gulisano
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *  
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *  
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *  
 *  Contact: Vincenzo Gulisano info@vincenzogulisano.com
 *
 */

package operator.in2;

import java.util.Collection;
import java.util.Objects;

import common.StreamConsumer;
import common.StreamProducer;
import common.component.ComponentState;
import common.component.ComponentState.BoxType;
import common.tuple.Tuple;
import operator.Operator;
import scheduling.priority.PriorityMetric;
import stream.Stream;
import stream.StreamFactory;

public abstract class BaseOperator2In<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple>
		implements Operator2In<IN, IN2, OUT> {

	private final ComponentState<Tuple, OUT> state;

	private final String INPUT1_KEY = "INPUT1";
	private final String INPUT2_KEY = "INPUT2";
	private final String OUTPUT_KEY = "OUTPUT";

	private final ProcessCommand2In<IN, IN2, OUT> processCommand = new ProcessCommand2In<>(this);

	public BaseOperator2In(String id, StreamFactory streamFactory) {
		state = new ComponentState<>(id, BoxType.OPERATOR2IN, streamFactory);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void registerIn(StreamProducer<IN> in) {
		state.setInput(INPUT1_KEY, (StreamProducer<Tuple>) in, this);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void registerIn2(StreamProducer<IN2> in) {
		state.setInput(INPUT2_KEY, (StreamProducer<Tuple>) in, this);
	}

	@Override
	public Collection<StreamConsumer<OUT>> getNext() {
		return state.getNext();
	}

	@Override
	public Collection<StreamProducer<? extends Tuple>> getPrevious() {
		return state.getPrevious();
	}

	@Override
	public Stream<OUT> getOutputStream(String reqId) {
		return state.getOutputStream(OUTPUT_KEY, this);
	}

	@Override
	public void addOutput(StreamConsumer<OUT> out) {
		state.setOutput(OUTPUT_KEY, out, this);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stream<IN> getInputStream(String reqId) {
		return (Stream<IN>) state.getInputStream(INPUT1_KEY);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Stream<IN2> getInput2Stream(String reqId) {
		return (Stream<IN2>) state.getInputStream(INPUT2_KEY);
	}

	@Override
	public void run() {
		processCommand.run();
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
	public Operator<IN2, OUT> secondInputView() {
		return new SecondInputOperator2InAdapter<>(this);
	}

	@Override
	public String getId() {
		return state.getId();
	}

	@Override
	public int getIndex() {
		return state.getIndex();
	}

	@Override
	public boolean hasInput() {
		return state.hasInput();
	}

	@Override
	public boolean hasOutput() {
		return state.hasOutput();
	}

	@Override
	public void onScheduled() {
	}

	public void onRun() {
	}

	@Override
	public void setPriorityMetric(PriorityMetric metric) {
		processCommand.setMetric(metric);
	}

	@Override
	public String toString() {
		return getId();
	}

	@Override
	public int hashCode() {
		return Objects.hash(state);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof BaseOperator2In)) {
			// Give the other object a chance to check equality. Useful in the case of
			// adapters
			return obj.equals(this);
		}
		BaseOperator2In<?, ?, ?> other = (BaseOperator2In<?, ?, ?>) obj;
		return Objects.equals(state, other.state);
	}

}
