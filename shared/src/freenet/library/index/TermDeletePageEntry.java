/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package freenet.library.index;

import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import freenet.library.io.FreenetURI;

/**
** A {@link TermPageEntry} that is marked for deletion.
*/
public class TermDeletePageEntry extends TermPageEntry {

	/**
	** For serialisation.
	*/
	public TermDeletePageEntry(String s, float r, Object u, String t, Set<Integer> pos, Map<Integer, String> p) {
		super(s, r, u, t, pos, p);
	}

	public TermDeletePageEntry(String s, float r, FreenetURI u, String t, Map<Integer, String> p) {
		super(s, r, u, t, p);
	}

	/**
	 * Convert from a TermPageEntry.
	 */
	public TermDeletePageEntry(TermPageEntry e) {
		super(e, e.rel);;
	}

	/**
	 * Convert to a TermPageEntry.
	 */
	public TermPageEntry toTermPageEntry() {
		return new TermPageEntry(this, rel);
	}

	/*========================================================================
	  abstract public class TermEntry
	 ========================================================================*/

	@Override public EntryType entryType() {
		assert(getClass() == TermDeletePageEntry.class);
		return EntryType.DELETE_PAGE;
	}

}
