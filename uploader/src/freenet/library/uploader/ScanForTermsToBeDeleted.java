/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package freenet.library.uploader;

import java.io.File;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import freenet.library.Priority;
import freenet.library.index.ProtoIndex;
import freenet.library.index.ProtoIndexComponentSerialiser;
import freenet.library.index.ProtoIndexSerialiser;
import freenet.library.index.TermDeletePageEntry;
import freenet.library.index.TermEntry;
import freenet.library.index.TermPageEntry;
import freenet.library.util.SkeletonBTreeSet;
import freenet.library.io.FreenetURI;
import freenet.library.io.serial.LiveArchiver;
import freenet.library.io.serial.Serialiser.PullTask;
import freenet.library.util.exec.SimpleProgress;
import freenet.library.util.exec.TaskAbortException;
import static freenet.library.uploader.DirectoryUploader.readStringFrom;
import static freenet.library.uploader.DirectoryUploader.LAST_URL_FILENAME;
import static freenet.library.uploader.Merger.TO_BE_DELETED;

public class ScanForTermsToBeDeleted {
	ProtoIndexSerialiser srl = null;
	String lastDiskIndexName;
	ProtoIndex idxFreenet;
	private FreenetURI lastUploadURI;
	File whereToWrite;
	private File directory;
	private int lastFoundNumber;
	private TermEntryFileWriter openedFile;
	private static int CREATED_FILES = 3;

	public ScanForTermsToBeDeleted(File dir, int lastFound) {
		directory = dir;
		lastFoundNumber = lastFound;
		openedFile = null;
		try {
			lastUploadURI = new FreenetURI(readStringFrom(new File(LAST_URL_FILENAME)));
		} catch (MalformedURLException e) {
			throw new RuntimeException("File contents of " + LAST_URL_FILENAME + " invalid.", e);
		}
		setupFreenetCacheDir();
		makeFreenetSerialisers();
	}

	private void writeTermEntry(TermPageEntry tpe) {
		if (openedFile == null) {
			lastFoundNumber ++;
			String restFilename = TO_BE_DELETED + lastFoundNumber;

			Map<String, String> emptyHeader = new HashMap<String, String>();
			openedFile = new TermEntryFileWriter(emptyHeader , new File(directory, restFilename));
		}
		openedFile.write(tpe);
	}

	public void run() throws TaskAbortException {
		Map<String, Long> seenUsks = new HashMap<String, Long>();
		int countFilledFiles = 0;
		int count = 0;
		for (Iterator<String> i = idxFreenet.ttab.keySetAutoDeflate().iterator();
				i.hasNext();) {
			count++;
			String term = i.next();
			System.out.println("" + count + " " + term);
			idxFreenet.ttab.inflate(term);
			SkeletonBTreeSet<TermEntry> set = idxFreenet.ttab.get(term);
			set.inflate();
			Map<String, TermPageEntry> usksInThisTerm = new HashMap<String, TermPageEntry>();
			for (TermEntry e : set) {
				if (e instanceof TermPageEntry) {
					TermPageEntry tpe = (TermPageEntry) e;
					if (tpe.toBeDropped()) {
						writeTermEntry(new TermDeletePageEntry(tpe));
						continue;
					}
					FreenetURI uri = tpe.getPage();
					if (uri.isSSKForUSK()) {
						FreenetURI uri2 = uri.uskForSSK();
						String root = uri2.getRoot();
						long edition = uri2.getEdition();
						if (seenUsks.containsKey(root)) {
							if (seenUsks.get(root) > edition) {
								// This term can be removed.
								writeTermEntry(new TermDeletePageEntry(tpe));
							} else if (edition > seenUsks.get(root)) {
								seenUsks.put(root, edition);
								if (usksInThisTerm.containsKey(root)) {
									// The old term can be removed.
									writeTermEntry(new TermDeletePageEntry(usksInThisTerm.get(root)));
									usksInThisTerm.put(root, tpe);
								}
							}
						} else {
							seenUsks.put(root, edition);
							usksInThisTerm.put(root, tpe);
						}
					}
				}
			}
			set.deflate();
			// Do one file full of removals at the time.
			if (openedFile != null && openedFile.isFull()) {
				openedFile.close();
				openedFile = null;
				countFilledFiles ++;
				if (countFilledFiles >= CREATED_FILES) {
					break;
				}
			}
		}
		if (openedFile != null) {
			openedFile.close();
			openedFile = null;
		}
	}

	private void setupFreenetCacheDir() {
		File dir = new File(UploaderPaths.LIBRARY_CACHE);
		dir.mkdir();
	}

	/**
	 * Setup the serialisers for reading from files. These convert
	 * tree nodes to and from blocks on Freenet, essentially.
	 */
	private void makeFreenetSerialisers() {
		if(srl == null) {
			srl = ProtoIndexSerialiser.forIndex(lastUploadURI, Priority.Bulk);
			LiveArchiver<Map<String,Object>,SimpleProgress> archiver =
				(LiveArchiver<Map<String,Object>,SimpleProgress>)(srl.getChildSerialiser());
			ProtoIndexComponentSerialiser leafsrl = ProtoIndexComponentSerialiser.get(ProtoIndexComponentSerialiser.FMT_DEFAULT, archiver);
			if(lastUploadURI == null) {
				try {
					idxFreenet = new ProtoIndex(new FreenetURI("CHK@"), "test", null, null, 0L);
				} catch (MalformedURLException e) {
					throw new AssertionError(e);
				}
				// FIXME more hacks: It's essential that we use the
				// same FreenetArchiver instance here.
				leafsrl.setSerialiserFor(idxFreenet);
			} else {
				try {
					PullTask<ProtoIndex> pull = new PullTask<ProtoIndex>(lastUploadURI);
					System.out.println("Pulling previous index "+lastUploadURI+" but unsure if it is needed.");
					srl.pull(pull);
					idxFreenet = pull.data;
					if(idxFreenet.getSerialiser().getLeafSerialiser() != archiver)
						throw new IllegalStateException("Different serialiser: "+idxFreenet.getSerialiser()+" should be "+leafsrl);
				} catch (TaskAbortException e) {
					System.err.println("Failed to download previous index for spider update: "+e);
					e.printStackTrace();
					return;
				}
			}
		}
	}
}
