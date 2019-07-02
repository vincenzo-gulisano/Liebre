/*  Copyright (C) 2015  Ioannis Nikolakopoulos,  
 * 			Daniel Cederman, 
 * 			Vincenzo Gulisano,
 * 			Marina Papatriantafilou,
 * 			Philippas Tsigas
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
 *  Contact: Ioannis (aka Yiannis) Nikolakopoulos ioaniko@chalmers.se
 *  	     Vincenzo Gulisano vincenzo.gulisano@chalmers.se
 *
 */

package common.scalegate;

import java.util.Random;

import common.tuple.RichTuple;

public class ScaleGateAArrImpl<T extends RichTuple> implements ScaleGate<T> {

	final int maxlevels;
	SGNodeAArrImpl<T> head;
	final SGNodeAArrImpl<T> tail;

	final int numberOfWriters;
	final int numberOfReaders;
	// Arrays of source/reader id local data
	WriterThreadLocalData[] writertld;
	ReaderThreadLocalData[] readertld;

	@SuppressWarnings("unchecked")
	public ScaleGateAArrImpl(int maxlevels, int writers, int readers) {
		this.maxlevels = maxlevels;

		this.head = new SGNodeAArrImpl<T>(maxlevels, null, null, -1);
		this.tail = new SGNodeAArrImpl<T>(maxlevels, null, null, -1);

		for (int i = 0; i < maxlevels; i++)
			head.setNext(i, tail);

		this.numberOfWriters = writers;
		this.numberOfReaders = readers;

		writertld = (WriterThreadLocalData[]) new Object[numberOfWriters];
		for (int i = 0; i < numberOfWriters; i++) {
			writertld[i] = new WriterThreadLocalData(head);
		}

		readertld = (ReaderThreadLocalData[]) new Object[numberOfReaders];
		for (int i = 0; i < numberOfReaders; i++) {
			readertld[i] = new ReaderThreadLocalData(head);
		}

		// This should not be used again, only the writer/reader-local variables
		head = null;
	}

	@Override
	/*
	 * (non-Javadoc)
	 */
	public T getNextReadyTuple(int readerID) {
		SGNodeAArrImpl<T> next = getReaderLocal(readerID).localHead.getNext(0);

		if (next != tail && !next.isLastAdded()) {
			getReaderLocal(readerID).localHead = next;
			return next.getTuple();
		}
		return null;
	}

	@Override
	// Add a tuple
	public void addTuple(T tuple, int writerID) {
		this.internalAddTuple(tuple, writerID);
	}

	private void insertNode(SGNodeAArrImpl<T> fromNode,
			SGNodeAArrImpl<T> newNode, final T obj, final int level) {
		while (true) {
			SGNodeAArrImpl<T> next = fromNode.getNext(level);
			if (next == tail || next.getTuple().compareTo(obj) > 0) {
				newNode.setNext(level, next);
				if (fromNode.trySetNext(level, next, newNode)) {
					break;
				}
			} else {
				fromNode = next;
			}
		}
	}

	private SGNodeAArrImpl<T> internalAddTuple(T obj, int inputID) {
		int levels = 1;
		WriterThreadLocalData ln = getWriterLocal(inputID);

		while (ln.rand.nextBoolean() && levels < maxlevels)
			levels++;

		SGNodeAArrImpl<T> newNode = new SGNodeAArrImpl<T>(levels, obj, ln,
				inputID);
		SGNodeAArrImpl<T>[] update = ln.update;
		SGNodeAArrImpl<T> curNode = update[maxlevels - 1];

		for (int i = maxlevels - 1; i >= 0; i--) {
			SGNodeAArrImpl<T> tx = curNode.getNext(i);

			while (tx != tail && tx.getTuple().compareTo(obj) < 0) {
				curNode = tx;
				tx = curNode.getNext(i);
			}

			update[i] = curNode;
		}

		for (int i = 0; i < levels; i++) {
			this.insertNode(update[i], newNode, obj, i);
		}

		ln.written = newNode;
		return newNode;
	}

	private WriterThreadLocalData getWriterLocal(int writerID) {
		return writertld[writerID];
	}

	private ReaderThreadLocalData getReaderLocal(int readerID) {
		return readertld[readerID];
	}

	protected class WriterThreadLocalData {
		// reference to the last written node by the respective writer
		volatile SGNodeAArrImpl<T> written;
		SGNodeAArrImpl<T>[] update;
		final Random rand;

		@SuppressWarnings("unchecked")
		public WriterThreadLocalData(SGNodeAArrImpl<T> localHead) {
			update = (SGNodeAArrImpl<T>[]) new Object[maxlevels];
			written = localHead;
			for (int i = 0; i < maxlevels; i++) {
				update[i] = localHead;
			}
			rand = new Random();
		}
	}

	protected class ReaderThreadLocalData {
		SGNodeAArrImpl<T> localHead;

		public ReaderThreadLocalData(SGNodeAArrImpl<T> lhead) {
			localHead = lhead;
		}
	}
}
