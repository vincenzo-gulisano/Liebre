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

package statistic;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class AvgStat {

	private long sum;
	private long count;

	PrintWriter out;

	long prevSec;

	public AvgStat(String outputFile, boolean autoFlush) {
		this.sum = 0;
		this.count = 0;

		FileWriter outFile;
		try {
			outFile = new FileWriter(outputFile);
			out = new PrintWriter(outFile, autoFlush);
		} catch (IOException e) {
			e.printStackTrace();
		}

		prevSec = System.currentTimeMillis() / 1000;

	}

	public void add(long v) {

		long thisSec = System.currentTimeMillis() / 1000;
		while (prevSec < thisSec) {
			out.println(prevSec * 1000 + "," + (count != 0 ? sum / count : -1));
			sum = 0;
			count = 0;
			prevSec++;
		}

		sum += v;
		count++;

	}

	public void close() {
		out.close();
	}

}
