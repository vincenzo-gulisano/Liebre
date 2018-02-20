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

import java.util.List;

import common.StreamProducer;
import common.tuple.Tuple;
import operator.Operator;
import operator.in1.Operator1In;
import stream.Stream;

public interface Operator2In<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple> extends Operator1In<IN, OUT> {

	void registerIn2(StreamProducer<IN2> in);

	List<OUT> processTupleIn2(IN2 tuple);

	Stream<IN2> getInput2Stream(String reqId);

	Operator<IN2, OUT> secondInputView();

	void recordTuple2Read(IN2 tuple, Stream<IN2> input);
}
