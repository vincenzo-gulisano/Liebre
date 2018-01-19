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

package common.util;

public class Util {

	public static volatile long LOOPS_PER_MILLI = 100000;

	public static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
			// Restore interruption status for thread
			Thread.currentThread().interrupt();
		}
	}

	public static long busySleep(long millis) {
		long dummy = 0;
		for (long i = 0; i < millis * LOOPS_PER_MILLI; i++) {
			dummy = (dummy + System.currentTimeMillis()) % millis;
		}
		return dummy;
	}

}
