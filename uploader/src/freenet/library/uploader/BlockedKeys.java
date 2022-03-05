package freenet.library.uploader;

import java.io.File;

import freenet.library.io.FreenetURI;

/**
 * Class that is the interface to a persistent store of keys that
 * shall not be considered for deletion.
 *
 * In the first implementation this is a list of lines with one
 * FreenetURI on each line.
 */
class BlockedKeys extends StoredKeys {

	/**
	 * Constructor.
	 * @param directory 
	 * @param doAll will reset the list.
	 */
	
	public BlockedKeys(File dir, boolean doAll) {
		super(dir, UploaderPaths.BASE_FILENAME_DATA + "blocked", !doAll);
	}

	public void block(FreenetURI page) {
		list.add(page);
	}

	public void flush() {
		super.flush();
		System.out.println("Now " + list.size() + " URIs in the block queue.");
	}
}
