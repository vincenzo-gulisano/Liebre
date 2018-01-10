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

package operator.in1;

import java.util.List;

import common.statistic.AverageStatistic;
import common.statistic.CountStatistic;
import common.tuple.Tuple;

public class Operator1InStatistic<IN extends Tuple, OUT extends Tuple> extends Operator1InDecorator<IN, OUT> {

	private final AverageStatistic processingTimeStat;
	private final CountStatistic executionsStat;

	public Operator1InStatistic(Operator1In<IN, OUT> operator, String outputFile) {
		this(operator, outputFile, true);
	}

	public Operator1InStatistic(Operator1In<IN, OUT> operator, String outputFile, boolean autoFlush) {
		super(operator);
		this.processingTimeStat = new AverageStatistic(outputFile, autoFlush);
		// FIXME: Remove hardcoded string!
		this.executionsStat = new CountStatistic(outputFile + ".runs", autoFlush);
	}

	@Override
	public void enable() {
		super.enable();
		processingTimeStat.enable();
		executionsStat.enable();
	}

	@Override
	public void disable() {
		processingTimeStat.disable();
		executionsStat.disable();
		super.disable();
	}

	@Override
	public List<OUT> processTupleIn1(IN tuple) {
		long start = System.nanoTime();
		List<OUT> outTuples = super.processTupleIn1(tuple);
		processingTimeStat.append(System.nanoTime() - start);
		executionsStat.append(1L);
		return outTuples;
	}

}
