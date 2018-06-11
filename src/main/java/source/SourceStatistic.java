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

package source;

import common.statistic.CountStatistic;
import common.tuple.Tuple;
import common.util.StatisticFilename;
import stream.StreamFactory;

public class SourceStatistic<T extends Tuple> extends SourceDecorator<T> {

	private final CountStatistic processingTimeStatistic;
	private final CountStatistic processedTuplesStatistic;
	private final CountStatistic timesScheduledStatistic;
	private final CountStatistic timesRunStatistic;

	public SourceStatistic(Source<T> source, StreamFactory streamFactory, String outputFolder) {
		this(source, streamFactory, outputFolder, true);
	}

	public SourceStatistic(Source<T> source, StreamFactory streamFactory, String outputFolder, boolean autoFlush) {
		super(source);
		this.processingTimeStatistic = new CountStatistic(StatisticFilename.INSTANCE.get(outputFolder, source, "proc"),
				autoFlush);
		this.processedTuplesStatistic = new CountStatistic(
				StatisticFilename.INSTANCE.get(outputFolder, source, "tuples"), autoFlush);
		this.timesScheduledStatistic = new CountStatistic(StatisticFilename.INSTANCE.get(outputFolder, source, "sched"),
				autoFlush);
		this.timesRunStatistic = new CountStatistic(StatisticFilename.INSTANCE.get(outputFolder, source, "runs"),
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
	public T getNextTuple() {
		long start = System.nanoTime();
		T tuple = super.getNextTuple();
		processingTimeStatistic.append(System.nanoTime() - start);
		processedTuplesStatistic.append(1L);
		return tuple;
	}

}
