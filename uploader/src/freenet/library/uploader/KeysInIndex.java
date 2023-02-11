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
 * Class that is the interface to a persistent store of USKs that
 * is the last version of a key.
 * All keys older than this one can be considered for removal.
 */
class KeysInIndex extends StoredKeys {

	/**
	 * Map of edition numbers. The URIs are USKs with edition 0.
	 */
	Map<FreenetURI, Long> editions = new HashMap<FreenetURI, Long>();

	/**
	 * Constructor.
	 * @param dir is the directory where the file is found and stored.
	 */
	
	public KeysInIndex(File dir) {
		super(dir, UploaderPaths.BASE_FILENAME_DATA + "keysinindex", true);
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

	protected KeysInIndex() {
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
			if (page.isUSK()) {
				FreenetURI replacedURI = updateEdition(page);
				if (replacedURI != null) {
					list.remove(replacedURI);
				}
				list.add(page);
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
				final long edition = page.getEdition();
				final long latestEdition = latestEdition(page);
				final boolean replaced = edition < latestEdition;
				return replaced;
			}
		} catch (MalformedURLException e) {
			// This is a strange problem. Lets remove this URI from the index.
			e.printStackTrace();
			return true;
		}
		// Other kinds of keys are never replaced.
		return false;
	}

	/**
	 * Is this USK in the index.
	 * @param uri The key to search for.
	 * @return
	 */
	public boolean contains(FreenetURI uri) {
		return list.contains(uri);
	}

	public void flush() {
		super.flush();
		System.out.println("Now " + list.size() + " URIs in the index.");
	}
}
