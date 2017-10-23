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

package operator2in;

import java.util.List;

import statistic.AvgStat;
import tuple.Tuple;

public class Operator2InStatistic<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple>
		extends BaseOperator2In<IN, IN2, OUT> {

	private BaseOperator2In<IN, IN2, OUT> operator;
	private AvgStat processingTimeStat;

	public Operator2InStatistic(BaseOperator2In<IN, IN2, OUT> operator,
			String outputFile) {
		this.operator = operator;
		this.processingTimeStat = new AvgStat(outputFile, true);
	}

	public Operator2InStatistic(BaseOperator2In<IN, IN2, OUT> operator,
			String outputFile, boolean autoFlush) {
		this.operator = operator;
		this.processingTimeStat = new AvgStat(outputFile, autoFlush);
	}

	@Override
	public void deActivate() {
		processingTimeStat.close();
		active = false;
	}

	public void process() {
		IN inTuple1 = in1.getNextTuple();
		if (inTuple1 != null) {
			long start = System.nanoTime();
			List<OUT> outTuples = this.operator.processTupleIn1(inTuple1);
			processingTimeStat.add(System.nanoTime() - start);
			if (outTuples != null) {
				for (OUT t : outTuples)
					out.addTuple(t);
			}
		}
		IN2 inTuple2 = in2.getNextTuple();
		if (inTuple2 != null) {
			long start = System.nanoTime();
			List<OUT> outTuples = this.operator.processTupleIn2(inTuple2);
			processingTimeStat.add(System.nanoTime() - start);
			if (outTuples != null) {
				for (OUT t : outTuples)
					out.addTuple(t);
			}
		}
	}

	@Override
	public List<OUT> processTupleIn1(IN tuple) {
		return null;
	}

	@Override
	public List<OUT> processTupleIn2(IN2 tuple) {
		return null;
	}
}
