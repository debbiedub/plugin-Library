package freenet.library.uploader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import freenet.library.io.FreenetURI;

/**
 * Class that is the interface to a persistant store of keys that
 * shall not be considered for deletion.
 *
 * In the first implementation this is a list of lines with one
 * FreenetURI on each line.
 */
class BlockedKeys {

	private static String filename = UploaderPaths.BASE_FILENAME_DATA + "blocked";

	private Set<FreenetURI> list = new HashSet<FreenetURI>();

	private File directory;
	/**
	 * Constructor.
	 * @param directory 
	 * @param doAll will reset the list.
	 */
	
	public BlockedKeys(File dir, boolean doAll) {
		directory = dir;
		if (!doAll) {
			// Read all
			throw new RuntimeException("Not yet implemented");
		}
	}

	public void block(FreenetURI page) {
		list.add(page);
	}

	public void flush() {
		String newFilename = filename + ".new";
		File newFile = new File(directory, newFilename);
		FileWriter fw = null;
		try {
			fw = new FileWriter(newFile, false);
			for (FreenetURI uri : list) {
				fw.write(uri.toString() + "\n");
			}
			fw.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		} finally {
			if (fw != null) {
				try {
					fw.close();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}
		}
		final File file = new File(directory, filename);
		file.delete();
		newFile.renameTo(file);
		System.out.println("Put " + list.size() + " URIs in the block queue.");
	}

}
