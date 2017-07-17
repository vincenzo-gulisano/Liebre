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

public class OperatorStatistic<T1 extends Tuple, T2 extends Tuple> extends
		BaseOperator<T1, T2> {

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
	public void deActivate() {
		processingTimeStat.close();
		active = false;
	}

//	public void process() {
//		long start = System.nanoTime();
//		this.operator.process();
//		processingTimeStat.add(System.nanoTime() - start);
//	}

	@Override
	public List<T2> processTuple(T1 tuple) {
		long start = System.nanoTime();
		this.operator.process();
		processingTimeStat.add(System.nanoTime() - start);
		return null;
	}
}
