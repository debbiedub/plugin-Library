/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package freenet.library.uploader;

import java.io.File;
import java.net.MalformedURLException;
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

	/**
	 * @param page Page to search for.
	 * @return the latest edition number for the page or -1 if not found.
	 */
	long latestEdition(FreenetURI page) {
		FreenetURI zeroedURI = page.setSuggestedEdition(0L);
		return editions.getOrDefault(zeroedURI, -1L);
	}

	FreenetURI updateEdition(FreenetURI page) {
		FreenetURI zeroedURI = page.setSuggestedEdition(0L);
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
		try {
			// Only USKs are considered.
			// SSKs are removed as if they were outdated USKs.
			if (page.isUSK()) {
				FreenetURI replacedURI = updateEdition(page);
				if (replacedURI != null) {
					list.remove(replacedURI);
					list.add(page);
				}
			} else if (page.isSSKForUSK()) {
				add(page.uskForSSK());
			}
		} catch (MalformedURLException e) {
			// This should not happen. Lets ignore this URI.
			e.printStackTrace();
		}
	}

	/**
	 * @param page to consider
	 * @return true if there is a newer USK
	 */
	public boolean isReplaced(FreenetURI page) {
		try {
			if (page.isUSK()) {
				return page.getEdition() < latestEdition(page);
			} else if(page.isSSKForUSK() ) {
				final FreenetURI usk = page.uskForSSK();
				if (isReplaced(usk)) {
					return true;
				}
				if (list.contains(usk)) {
					return true;
				}
				return false;
			}
		} catch (MalformedURLException e) {
			// This is a strange problem. Lets remove this URI from the index.
			e.printStackTrace();
			return true;
		}
		// Other kinds of keys are never replaced.
		return false;
	}

	public void flush() {
		super.flush();
		System.out.println("Now " + list.size() + " URIs in the index.");
	}
}
