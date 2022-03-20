/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package freenet.library.uploader;

import java.io.File;

import freenet.library.io.FreenetURI;

/**
 * Class that is the interface to a persistent store of keys that
 * shall not be considered for deletion.
 */
class BlockedKeys extends StoredKeys {

	/**
	 * Constructor.
	 * @param directory 
	 * @param doAll will reset the list.
	 */
	
	public BlockedKeys(File dir, boolean doAll) {
		super(dir, UploaderPaths.BASE_FILENAME_DATA + "keysblocked", !doAll);
	}

	public void block(FreenetURI page) {
		list.add(page);
	}

	public void flush() {
		super.flush();
		System.out.println("Now " + list.size() + " URIs in the block queue.");
	}

	public boolean isBlocked(FreenetURI uri) {
		return list.contains(uri);
	}
}
