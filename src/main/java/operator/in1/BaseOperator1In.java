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

import common.component.ComponentType;
import common.tuple.Tuple;
import operator.AbstractOperator;
import scheduling.priority.PriorityMetric;
import stream.StreamFactory;

public abstract class BaseOperator1In<IN extends Tuple, OUT extends Tuple> extends AbstractOperator<IN, OUT>
		implements Operator1In<IN, OUT> {

	private final String INPUT_KEY = "INPUT";
	private final String OUTPUT_KEY = "OUTPUT";
	private final ProcessCommand1In<IN, OUT> processCommand = new ProcessCommand1In<>(this);

	protected BaseOperator1In(String id, ComponentType type) {
		super(id, type);
	}

	public BaseOperator1In(String id, StreamFactory streamFactory) {
		this(id, ComponentType.OPERATOR);
	}

	@Override
	public void run() {
		processCommand.run();
	}

	@Override
	public void setPriorityMetric(PriorityMetric metric) {
		processCommand.setMetric(metric);
	}
}
