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

import common.statistic.AverageStatistic;
import common.statistic.CountStatistic;
import common.tuple.Tuple;
import common.util.StatisticFilename;

public class Operator2InStatistic<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple>
		extends Operator2InDecorator<IN, IN2, OUT> {

	private final AverageStatistic processingTimeStat;
	private final CountStatistic executionsStat;

	public Operator2InStatistic(Operator2In<IN, IN2, OUT> operator, String outputFolder) {
		this(operator, outputFolder, true);
	}

	public Operator2InStatistic(Operator2In<IN, IN2, OUT> operator, String outputFolder, boolean autoFlush) {
		super(operator);
		this.processingTimeStat = new AverageStatistic(StatisticFilename.INSTANCE.get(outputFolder, operator, "proc"),
				autoFlush);
		this.executionsStat = new CountStatistic(StatisticFilename.INSTANCE.get(outputFolder, operator, "runs"),
				autoFlush);
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

	@Override
	public List<OUT> processTupleIn2(IN2 tuple) {
		long start = System.nanoTime();
		List<OUT> outTuples = super.processTupleIn2(tuple);
		processingTimeStat.append(System.nanoTime() - start);
		executionsStat.append(1L);
		return outTuples;
	}
}
