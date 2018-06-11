/*
 * Copyright (C) 2017-2018
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

package scheduling.priority;

import java.util.List;

import common.component.Component;

public enum PriorityMetricFactory {
	STIMULUS_MATRIX {
		@Override
		public PriorityMetric newInstance(List<Component> tasks, List<Component> passiveTasks, int nThreads) {
			return new StimulusMatrixMetric(tasks, passiveTasks, nThreads);
		}
	},
	QUEUE_SIZE_MATRIX {
		@Override
		public PriorityMetric newInstance(List<Component> tasks, List<Component> passiveTasks, int nThreads) {
			return new QueueSizeMatrixMetric(tasks, passiveTasks, nThreads);
		}
	},
	STIMULUS {
		@Override
		public PriorityMetric newInstance(List<Component> tasks, List<Component> passiveTasks, int nThreads) {
			return new StimulusMetric(tasks, passiveTasks);
		}
	},
	QUEUE_SIZE {
		@Override
		public PriorityMetric newInstance(List<Component> tasks, List<Component> passiveTasks, int nThreads) {
			return new QueueSizeMetric(tasks, passiveTasks);
		}

	},
	CONTROL {
		@Override
		public PriorityMetric newInstance(List<Component> tasks, List<Component> passiveTasks, int nThreads) {
			return new ControlPriorityMetric(tasks, passiveTasks);
		}
	};
	public abstract PriorityMetric newInstance(List<Component> tasks, List<Component> passiveTasks,
			int nThreads);
}
