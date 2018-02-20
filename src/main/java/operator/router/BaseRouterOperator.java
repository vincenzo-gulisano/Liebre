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

package operator.router;

import java.util.List;

import common.BoxState.BoxType;
import common.StreamConsumer;
import common.StreamProducer;
import common.tuple.Tuple;
import operator.AbstractOperator;
import stream.Stream;
import stream.StreamFactory;

public class BaseRouterOperator<T extends Tuple> extends AbstractOperator<T, T> implements RouterOperator<T> {

	protected RouterFunction<T> router;
	private static final String INPUT_KEY = "INPUT";
	private final ProcessCommandRouter<T> processCommand = new ProcessCommandRouter<>(this);

	public BaseRouterOperator(String id, StreamFactory streamFactory, RouterFunction<T> router) {
		super(id, BoxType.ROUTER, streamFactory);
		this.router = router;
	}

	@Override
	public void addOutput(StreamConsumer<T> out) {
		state.setOutput(out.getId(), out, this);
	}

	@Override
	public List<String> chooseOperators(T tuple) {
		return router.chooseOperators(tuple);
	}

	@Override
	public Stream<T> getOutputStream(String reqId) {
		return state.getOutputStream(reqId, this);
	}

	@Override
	public void registerIn(StreamProducer<T> in) {
		state.setInput(INPUT_KEY, in, this);
	}

	@Override
	public Stream<T> getInputStream(String requestorId) {
		return state.getInputStream(INPUT_KEY);
	}

	@Override
	public void run() {
		processCommand.run();
	}

	@Override
	public void recordTupleRead(T tuple, Stream<T> input) {
		state.recordTupleRead(tuple, input);
	}

	@Override
	public void recordTupleWrite(T tuple, Stream<T> output) {
		state.recordTupleWrite(tuple, output);
	}

}
