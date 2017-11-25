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

import common.statistic.AverageStatistic;
import common.tuple.Tuple;

//FIXME: this is a really bad mix of composition and inheritance
// This class should only implement operator
public class OperatorStatistic<IN extends Tuple, OUT extends Tuple> extends BaseOperator<IN, OUT> {

	private BaseOperator<IN, OUT> operator;
	private AverageStatistic processingTimeStat;

	public OperatorStatistic(BaseOperator<IN, OUT> operator, String outputFile) {
		this(operator, outputFile, true);
	}

	public OperatorStatistic(BaseOperator<IN, OUT> operator, String outputFile, boolean autoFlush) {
		super(operator.getId(), operator.state.getStreamFactory());
		this.operator = operator;
		this.processingTimeStat = new AverageStatistic(outputFile, autoFlush);
	}

	@Override
	public void enable() {
		processingTimeStat.enable();
		super.enable();
	}

	@Override
	public void disable() {
		processingTimeStat.disable();
		super.disable();
	}

	@Override
	public List<OUT> processTuple(IN tuple) {
		long start = System.nanoTime();
		List<OUT> outTuples = this.operator.processTuple(tuple);
		processingTimeStat.append(System.nanoTime() - start);
		return outTuples;
	}

}
