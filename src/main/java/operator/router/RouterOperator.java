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

import common.StreamConsumer;
import common.BoxState.BoxType;
import common.tuple.Tuple;
import operator.BaseOperator;
import stream.Stream;
import stream.StreamFactory;

public class RouterOperator<T extends Tuple> extends BaseOperator<T, T> {

	protected RouterFunction<T> router;

	public RouterOperator(String id, StreamFactory streamFactory, RouterFunction<T> router) {
		super(id, BoxType.ROUTER, streamFactory);
		this.router = router;
	}

	@Override
	public void addOutput(StreamConsumer<T> out) {
		state.setOutput(out.getId(), out, this);
	}

	@Override
	public void process() {
		T inTuple = getInputStream(getId()).getNextTuple();
		if (inTuple != null) {
			List<String> streams = router.chooseOperators(inTuple);
			if (streams != null)
				for (String operator : router.chooseOperators(inTuple))
					state.getOutputStream(operator, this).addTuple(inTuple);
		}
	}

	@Override
	public Stream<T> getOutputStream(String reqId) {
		return state.getOutputStream(reqId, this);
	}

	@Override
	public List<T> processTuple(T tuple) {
		throw new UnsupportedOperationException();
	}

}
