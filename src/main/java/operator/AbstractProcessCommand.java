/*
 * Copyright (C) 2017-2019
 *   Vincenzo Gulisano
 *   Dimitris Palyvos-Giannas
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Contact:
 *   Vincenzo Gulisano info@vincenzogulisano.com
 *   Dimitris Palyvos-Giannas palyvos@chalmers.se
 */

package operator;

import common.component.ProcessCommand;

/**
 * Encapsulation of the execution logic for operators. This is required in order
 * to have reusable decorators without the need to duplicate code (i.e. the
 * process() function) and without having to resort to method interceptors.
 * <br/>
 * Note that similar classes exist for sources and sinks but for technical
 * reasons do not extend this.
 * 
 * @author palivosd
 *
 * @param <OP>
 *            The operator subclass used.
 */
public abstract class AbstractProcessCommand<OP extends Operator<?, ?>> implements ProcessCommand {
	protected final OP operator;

	protected AbstractProcessCommand(OP operator) {
		this.operator = operator;
	}

	@Override
	public final void run() {
		if (operator.isEnabled()) {
			process();
		}
	}

	@Override
	public abstract void process();

}
