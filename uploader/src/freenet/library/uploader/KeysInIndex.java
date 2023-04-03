/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package freenet.library.uploader;

import java.io.File;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
		int sizeBefore = list.size();
		Set<FreenetURI> copiedList = new HashSet<FreenetURI>();
		copiedList.addAll(list);
		list.clear();
		for (FreenetURI key : copiedList) {
			add(key);
		}
		System.out.println("Read " + sizeBefore + " URIs. " + list.size() + " are kept.");
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

	/**
	 * Add the page to editions or update the edition if a higher
	 * one is found.
	 * Also add t list and remove replaced pages from list.
	 * @param page the page to add.
	 * @return a FreenetURI of a page that is no longer valid
	 */
	public void add(FreenetURI page) {
		try {
			// Only USKs are considered.
			if (page.isUSK()) {
				FreenetURI zeroedURI = page.setSuggestedEdition(0L);
				long edition = page.getEdition();
				if (editions.containsKey(zeroedURI)) {
					final long foundEdition = editions.get(zeroedURI);
					if (edition > foundEdition) {
						list.remove(zeroedURI.setSuggestedEdition(foundEdition));
					} else {
					    return;
					}
				}
				editions.put(zeroedURI, edition);
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

	public static void main(String[] argv) {
		new KeysInIndex(new File("."));
	}
}
