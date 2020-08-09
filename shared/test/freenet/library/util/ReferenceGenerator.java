/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package freenet.library.util;

class ReferenceGenerator implements NumberGenerator {
	private static long lastNumber = 0;

	@Override
	public synchronized Long next() {
		return new Long(++lastNumber);
	}
}
