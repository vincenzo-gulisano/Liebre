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

package operator;

import java.util.List;

import statistic.AvgStat;
import tuple.Tuple;

public class OperatorStatistic<IN extends Tuple, OUT extends Tuple> extends
		BaseOperator<IN, OUT> {

	private BaseOperator<IN, OUT> operator;
	private AvgStat processingTimeStat;

	public OperatorStatistic(BaseOperator<IN, OUT> operator, String outputFile) {
		this.operator = operator;
		this.processingTimeStat = new AvgStat(outputFile, true);
	}

	public OperatorStatistic(BaseOperator<IN, OUT> operator, String outputFile,
			boolean autoFlush) {
		this.operator = operator;
		this.processingTimeStat = new AvgStat(outputFile, autoFlush);
	}

	@Override
	public void deActivate() {
		processingTimeStat.close();
		active = false;
	}

	public void process() {
		IN inTuple = in.getNextTuple();
		if (inTuple != null) {
			long start = System.nanoTime();
			List<OUT> outTuples = this.operator.processTuple(inTuple);
			processingTimeStat.add(System.nanoTime() - start);
			if (outTuples != null) {
				for (OUT t : outTuples)
					out.addTuple(t);
			}
		}
	}

	@Override
	public List<OUT> processTuple(IN tuple) {
		return null;
	}
}
