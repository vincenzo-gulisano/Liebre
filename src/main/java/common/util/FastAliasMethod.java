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

package common.util;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Random;

//FIXME: Cite author
public class FastAliasMethod {
	/* The random number generator used to sample from the distribution. */
	private final Random random;

	/* The probability and alias tables. */
	private final int[] alias;
	private final double[] probability;
	private final double average;
	private boolean enabled;
	private final Deque<Integer> small = new ArrayDeque<Integer>();
	private final Deque<Integer> large = new ArrayDeque<Integer>();

	public FastAliasMethod(int N, Random random) {
		if (N <= 0 || random == null) {
			throw new IllegalArgumentException();
		}
		this.alias = new int[N];
		this.probability = new double[N];
		this.average = 1.0 / N;
		this.random = random;
	}

	/**
	 * Initialize the Alias method. For efficiency reasons, the probabilities list
	 * provided will be altered. If you do not want this, provide a copy of the list
	 * instead.
	 * 
	 * @param probabilities
	 */
	public void init(List<Double> probabilities) {
		if (probabilities.size() != probability.length) {
			throw new IllegalArgumentException(String.format(
					"The Alias method has been initialized with size %d but got an list of size %d instead",
					alias.length, probabilities.size()));
		}
		/* Populate the stacks with the input probabilities. */
		for (int i = 0; i < probabilities.size(); ++i) {
			/*
			 * If the probability is below the average probability, then we add it to the
			 * small list; otherwise we add it to the large list.
			 */
			if (probabilities.get(i) >= average)
				large.add(i);
			else
				small.add(i);
		}

		/*
		 * As a note: in the mathematical specification of the algorithm, we will always
		 * exhaust the small list before the big list. However, due to floating point
		 * inaccuracies, this is not necessarily true. Consequently, this inner loop
		 * (which tries to pair small and large elements) will have to check that both
		 * lists aren't empty.
		 */
		while (!small.isEmpty() && !large.isEmpty()) {
			/* Get the index of the small and the large probabilities. */
			int less = small.removeLast();
			int more = large.removeLast();

			/*
			 * These probabilities have not yet been scaled up to be such that 1/n is given
			 * weight 1.0. We do this here instead.
			 */
			probability[less] = probabilities.get(less) * probabilities.size();
			alias[less] = more;

			/*
			 * Decrease the probability of the larger one by the appropriate amount.
			 */
			probabilities.set(more, (probabilities.get(more) + probabilities.get(less)) - average);

			/*
			 * If the new probability is less than the average, add it into the small list;
			 * otherwise add it to the large list.
			 */
			if (probabilities.get(more) >= average)
				large.add(more);
			else
				small.add(more);
		}

		/*
		 * At this point, everything is in one list, which means that the remaining
		 * probabilities should all be 1/n. Based on this, set them appropriately. Due
		 * to numerical issues, we can't be sure which stack will hold the entries, so
		 * we empty both.
		 */
		while (!small.isEmpty())
			probability[small.removeLast()] = 1.0;
		while (!large.isEmpty())
			probability[large.removeLast()] = 1.0;
		enabled = true;
	}

	/**
	 * Samples a value from the underlying distribution.
	 *
	 * @return A random value sampled from the underlying distribution.
	 */
	public int next() {
		if (!enabled) {
			throw new IllegalStateException("Call init() first!");
		}
		/* Generate a fair die roll to determine which column to inspect. */
		int column = random.nextInt(probability.length);

		/* Generate a biased coin toss to determine which option to pick. */
		boolean coinToss = random.nextDouble() < probability[column];

		/* Based on the outcome, return either the column or its alias. */
		return coinToss ? column : alias[column];
	}
}
