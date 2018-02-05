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

import common.statistic.CountStatistic;
import common.tuple.Tuple;
import common.util.StatisticFilename;

public class Operator2InStatistic<IN extends Tuple, IN2 extends Tuple, OUT extends Tuple>
		extends Operator2InDecorator<IN, IN2, OUT> {

	private final CountStatistic processingTimeStatistic;
	private final CountStatistic processedTuplesStatistic;
	private final CountStatistic timesScheduledStatistic;
	private final CountStatistic timesRunStatistic;

	public Operator2InStatistic(Operator2In<IN, IN2, OUT> operator, String outputFolder) {
		this(operator, outputFolder, true);
	}

	public Operator2InStatistic(Operator2In<IN, IN2, OUT> operator, String outputFolder, boolean autoFlush) {
		super(operator);
		this.processingTimeStatistic = new CountStatistic(
				StatisticFilename.INSTANCE.get(outputFolder, operator, "proc"), autoFlush);
		this.processedTuplesStatistic = new CountStatistic(
				StatisticFilename.INSTANCE.get(outputFolder, operator, "tuples"), autoFlush);
		this.timesScheduledStatistic = new CountStatistic(
				StatisticFilename.INSTANCE.get(outputFolder, operator, "sched"), autoFlush);
		this.timesRunStatistic = new CountStatistic(StatisticFilename.INSTANCE.get(outputFolder, operator, "runs"),
				autoFlush);
	}

	@Override
	public void enable() {
		super.enable();
		processingTimeStatistic.enable();
		timesScheduledStatistic.enable();
		timesRunStatistic.enable();
		processedTuplesStatistic.enable();
	}

	@Override
	public void disable() {
		processingTimeStatistic.disable();
		processedTuplesStatistic.disable();
		timesScheduledStatistic.disable();
		timesRunStatistic.disable();
		super.disable();
	}

	@Override
	public void onScheduled() {
		timesScheduledStatistic.append(1L);
		super.onScheduled();
	}

	@Override
	public void onRun() {
		timesRunStatistic.append(1L);
		super.onRun();
	}

	@Override
	public List<OUT> processTupleIn1(IN tuple) {
		long start = System.nanoTime();
		List<OUT> outTuples = super.processTupleIn1(tuple);
		processingTimeStatistic.append(System.nanoTime() - start);
		return outTuples;
	}

	@Override
	public List<OUT> processTupleIn2(IN2 tuple) {
		long start = System.nanoTime();
		List<OUT> outTuples = super.processTupleIn2(tuple);
		processingTimeStatistic.append(System.nanoTime() - start);
		processedTuplesStatistic.append(1L);
		return outTuples;
	}
}
