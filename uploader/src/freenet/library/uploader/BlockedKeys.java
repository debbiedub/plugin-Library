package freenet.library.uploader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.HashSet;
import java.util.Set;

import freenet.library.io.FreenetURI;

/**
 * Class that is the interface to a persistent store of keys that
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
			final File file = new File(directory, filename);
			try {
				FileReader fr = new FileReader(file);
				BufferedReader br = new BufferedReader(fr);
				String line;
				while ((line = br.readLine()) != null) {
					try {
						list.add(new FreenetURI(line));
					} catch (MalformedURLException e) {
						// This shouldn't happen. Ignore this URI.
					}
				}
			} catch (FileNotFoundException e) {
				// There is no such file. That is fine.
			} catch (IOException e) {
				// We suddenly couln't read this file. Strange problem.
				throw new RuntimeException(e);
			}
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
		System.out.println("Now " + list.size() + " URIs in the block queue.");
	}

}
