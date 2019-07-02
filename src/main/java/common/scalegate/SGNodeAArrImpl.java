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

import java.util.concurrent.atomic.AtomicReferenceArray;

import common.tuple.RichTuple;


public class SGNodeAArrImpl<T extends RichTuple> {

    final AtomicReferenceArray<SGNodeAArrImpl<T>> next;
    final T obj;
    final ScaleGateAArrImpl<T>.WriterThreadLocalData ln;
    final int writerID;
    volatile boolean assigned;
    
    public SGNodeAArrImpl (int levels, T t, ScaleGateAArrImpl<T>.WriterThreadLocalData ln, int writerID) {
	next = new AtomicReferenceArray<SGNodeAArrImpl<T>>(levels);
	for (int i = 0; i < levels; i++) {
	    	next.set(i, null);
	}
	this.obj = t;
	this.assigned = false;
	this.ln = ln;
	this.writerID = writerID;
    }
    
    public SGNodeAArrImpl<T> getNext(int level) {
	return next.get(level);
    }

    public T getTuple() {
	return this.obj;
    }

    public void setNext(int i, SGNodeAArrImpl<T> newNode) {
	next.set(i, newNode);
    }

    public boolean trySetNext(int i, SGNodeAArrImpl<T> oldNode,
	    SGNodeAArrImpl<T> newNode) {
	return next.compareAndSet(i, oldNode, newNode);
    }

    public boolean isLastAdded() {
	// read this as volatile
	return this == ln.written;
    }

}
