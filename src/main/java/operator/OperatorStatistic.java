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

import statistic.AvgStat;
import stream.Stream;
import tuple.Tuple;

public class OperatorStatistic<T1 extends Tuple, T2 extends Tuple> implements
		Operator<T1, T2> {

	protected Stream<T1> in;
	protected Stream<T2> out;
	protected boolean active = false;
	private BaseOperator<T1, T2> operator;
	private AvgStat processingTimeStat;

	public OperatorStatistic(BaseOperator<T1, T2> operator, String outputFile) {
		this.operator = operator;
		this.processingTimeStat = new AvgStat(outputFile, true);
	}

	public OperatorStatistic(BaseOperator<T1, T2> operator, String outputFile,
			boolean autoFlush) {
		this.operator = operator;
		this.processingTimeStat = new AvgStat(outputFile, autoFlush);
	}

	@Override
	public void registerIn(Stream<T1> in) {
		this.in = in;
	}

	@Override
	public void registerOut(Stream<T2> out) {
		this.out = out;
	}

	@Override
	public void run() {
		while (active) {
			process();
		}
	}

	@Override
	public void activate() {
		active = true;
	}

	@Override
	public void deActivate() {
		active = false;
	}

	protected void process() {
		long start = System.nanoTime();
		this.operator.process();
		processingTimeStat.add(System.nanoTime() - start);
	}
}
