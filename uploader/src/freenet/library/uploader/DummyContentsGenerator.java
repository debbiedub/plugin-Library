/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */

package freenet.library.uploader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import net.pterodactylus.fcp.FcpConnection;

import freenet.library.FactoryRegister;
import freenet.library.index.TermEntry;
import freenet.library.index.TermPageEntry;
import freenet.library.io.FreenetURI;
import freenet.library.util.exec.TaskAbortException;

/**
 * Generate a file that can be merged.
 */
final public class DummyContentsGenerator {

	public static void main(String[] argv) {
			File directory = new File(".");
			createDataFile(directory, argv[0]);
	}

	private static void createDataFile(File directory, String term) {
		final String[] existingFiles = Merger.getMatchingFiles(directory, UploaderPaths.BASE_FILENAME_PUSH_DATA);
		System.out.println("There is " + existingFiles.length + " files.");

		// Calculate the last number of filtered and processed files.
		int lastFoundNumber = 0;
		for (String filename : existingFiles) {
			int numberFound = Integer.parseInt(filename.substring(UploaderPaths.BASE_FILENAME_PUSH_DATA.length()));
			if (numberFound > lastFoundNumber) {
				lastFoundNumber = numberFound;
			}
		}

		String filename = UploaderPaths.BASE_FILENAME_PUSH_DATA + (lastFoundNumber + 1);
		System.out.println("File: " + filename);
		Map<String, String> header = new HashMap<String, String>();
		TermEntryFileWriter t = new TermEntryFileWriter(header, new File(directory, filename));

		Set<Integer> pos = new HashSet<Integer>();
		pos.add(1357);
		FreenetURI uri;
		try {
			uri = new FreenetURI("SSK@Isel-izOnLWWOygllc8sr~1reXQJz1LNGLIY-voagYQ,xWfr4I-BX7bolDe-kI3DW~i9xpy0YZqAQSHCHd-Bu9k,AQACAAE/fakesite-17/fake.html");
		} catch (MalformedURLException e) {
			throw new IllegalArgumentException("Failed fake URI.", e);
		}
		TermEntry tt = new TermPageEntry(term, (float) 0.0009, uri, "Fake page", pos, null);
		t.write(tt);
		t.close();
	}
}
