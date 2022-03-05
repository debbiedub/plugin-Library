/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package freenet.library.uploader;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import freenet.library.io.FreenetURI;

/**
 * Class that is the interface to a persistent store of keys that
 * is the last version of a key.
 * All keys older than this one can be considered for removal.
 *
 * For historic reasons (the use of SSKs instead of USKs in the index),
 * SSKs are handled as their corresponding USK.
 */
class KeysInIndex extends StoredKeys {

	/**
	 * Map of edition numbers. The URIs are USKs with edition 0.
	 */
	Map<FreenetURI, Long> editions = new HashMap<FreenetURI, Long>();

	/**
	 * Constructor.
	 * @param directory 
	 * @param doAll will reset the list.
	 */
	
	public KeysInIndex(File dir) {
		super(dir, UploaderPaths.BASE_FILENAME_DATA + "keysinindex", false);
		HashSet<FreenetURI> replacedList = new HashSet<FreenetURI>();
		for (FreenetURI key : list) {
			FreenetURI replacedURI = updateEdition(key);
			if (replacedURI != null) {
				replacedList.add(replacedURI);
			}
		}
		System.out.println("Read " + list.size() + " URIs. " + replacedList.size() + " are replaced.");
		list.removeAll(replacedList);
	}

	FreenetURI updateEdition(FreenetURI page) {
		FreenetURI zeroedURI = page.setSuggestedEdition(0);
		long edition = page.getEdition();
		if (editions.containsKey(zeroedURI)) {
			final long foundEdition = editions.get(zeroedURI);
			if (edition > foundEdition) {
				editions.put(zeroedURI, edition);
				return zeroedURI.setSuggestedEdition(foundEdition);
			}
			return null;
		}
		editions.put(zeroedURI, edition);
		return null;
	}

	public void add(FreenetURI page) {
		FreenetURI replacedURI = updateEdition(page);
		if (replacedURI != null) {
			list.remove(replacedURI);
			list.add(page);
		}
	}

	public void flush() {
		super.flush();
		System.out.println("Now " + list.size() + " URIs in the index.");
	}
}
