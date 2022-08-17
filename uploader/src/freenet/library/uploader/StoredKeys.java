/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
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
 * Class that is the interface to a persistent store of keys.
 *
 * In the first implementation this is a list of lines with one
 * FreenetURI on each line.
 */

public class StoredKeys {

	protected String filename;
	protected Set<FreenetURI> list = new HashSet<FreenetURI>();
	protected File directory;

	public StoredKeys(File dir, String name, boolean readFile) {
		directory= dir;
		filename = name;
		if (readFile) {
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
				br.close();
			} catch (FileNotFoundException e) {
				// There is no such file. That is fine.
			} catch (IOException e) {
				// We suddenly couldn't read this file. Strange problem.
				throw new RuntimeException(e);
			}
		}
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
	}

}