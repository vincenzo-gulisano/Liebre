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

package operator.map;

import java.util.List;

import common.tuple.Tuple;
import operator.in1.BaseOperator1In;
import stream.StreamFactory;

public class FlatMapOperator<IN extends Tuple, OUT extends Tuple> extends BaseOperator1In<IN, OUT> {

	private FlatMapFunction<IN, OUT> map;

	public FlatMapOperator(String id, StreamFactory streamFactory, FlatMapFunction<IN, OUT> map) {
		super(id, streamFactory);
		this.map = map;
	}

	@Override
	public List<OUT> processTupleIn1(IN tuple) {
		return map.map(tuple);
	}
}
