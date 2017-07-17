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

public class Operator2InStatistic<T1 extends Tuple, T2 extends Tuple, T3 extends Tuple>
		extends BaseOperator2In<T1, T2, T3> {

	private BaseOperator2In<T1, T2, T3> operator;
	private AvgStat processingTimeStat;

	public Operator2InStatistic(BaseOperator2In<T1, T2, T3> operator,
			String outputFile) {
		this.operator = operator;
		this.processingTimeStat = new AvgStat(outputFile, true);
	}

	public Operator2InStatistic(BaseOperator2In<T1, T2, T3> operator,
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
		T1 inTuple1 = in1.getNextTuple();
		if (inTuple1 != null) {
			long start = System.nanoTime();
			List<T3> outTuples = this.operator.processTupleIn1(inTuple1);
			processingTimeStat.add(System.nanoTime() - start);
			if (outTuples != null) {
				for (T3 t : outTuples)
					out.addTuple(t);
			}
		}
		T2 inTuple2 = in2.getNextTuple();
		if (inTuple2 != null) {
			long start = System.nanoTime();
			List<T3> outTuples = this.operator.processTupleIn2(inTuple2);
			processingTimeStat.add(System.nanoTime() - start);
			if (outTuples != null) {
				for (T3 t : outTuples)
					out.addTuple(t);
			}
		}
	}

	@Override
	public List<T3> processTupleIn1(T1 tuple) {
		return null;
	}

	@Override
	public List<T3> processTupleIn2(T2 tuple) {
		return null;
	}
}
