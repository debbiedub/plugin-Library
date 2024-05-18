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
	private int countFilledFiles;
	private static String NEW_TO_BE_DELETED = "library.new.deletes";

	/**
	 * Don't remove too many per term. There seems to be a problem
	 * with creating the index with entries to be removed and this
	 * is an attempt to address that without having do dig too deep
	 * in the details of how the B-tree is persisted.
	 */
	private static int MAX_ENTRIES_PER_TERM = 10000;

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
			File file = new File(directory, NEW_TO_BE_DELETED);
			file.delete();

			Map<String, String> emptyHeader = new HashMap<String, String>();
			openedFile = new TermEntryFileWriter(emptyHeader, file);
		}
		openedFile.write(tpe);
	}

	private void rotateFile() {
		File file = new File(directory, NEW_TO_BE_DELETED);
		lastFoundNumber ++;
		String restFilename = TO_BE_DELETED + lastFoundNumber;
		file.renameTo(new File(directory, restFilename));

		countFilledFiles ++;
	}

	public void run() throws TaskAbortException {
		countFilledFiles = 0;
		int termNumber = 0;
		int termOverflowCount = 0;
		int emptyTerms = 0;
		KeysInIndex keysInIndex = new KeysInIndex(directory);
		BlockedKeys blockedKeys = new BlockedKeys(directory, false);
		for (Iterator<String> i = idxFreenet.ttab.keySetAutoDeflate().iterator();
				i.hasNext();) {
			termNumber++;
			String term = i.next();
			System.out.print("" + termNumber + " " + term + "\r");
			idxFreenet.ttab.inflate(term);
			SkeletonBTreeSet<TermEntry> set = idxFreenet.ttab.get(term);
			set.inflate();
			int countWrittenEntriesThisTerm = 0;
			for (TermEntry e : set) {
				if (countWrittenEntriesThisTerm >= MAX_ENTRIES_PER_TERM) {
					termOverflowCount++;
					break;
				}
				if (e instanceof TermPageEntry) {
					TermPageEntry tpe = (TermPageEntry) e;
					if (tpe.toBeDropped()) {
						writeTermEntry(new TermDeletePageEntry(tpe));
						continue;
					}
					FreenetURI uri = tpe.getPage();
					keysInIndex.add(uri);
					if (blockedKeys.isBlocked(uri)) {
						continue;
					}
					if (keysInIndex.isReplaced(uri)) {
						// This term can be removed since there is a newer
						// page in the index.
						writeTermEntry(new TermDeletePageEntry(tpe));
						++countWrittenEntriesThisTerm;
						continue;
					}
					if (uri.isSSKForUSK()) {
						FreenetURI usk = uri.uskForSSK();
						long edition = usk.getEdition();
						if (blockedKeys.isBlocked(usk)) {
							continue;
						}
						if (blockedKeys.isBlocked(usk.setSuggestedEdition(edition + 1L))) {
							continue;
						}
						if (blockedKeys.isBlocked(usk.setSuggestedEdition(edition + 2L))) {
							continue;
						}
						if (blockedKeys.isBlocked(usk.setSuggestedEdition(edition + 3L))) {
							continue;
						}
						if (blockedKeys.isBlocked(usk.setSuggestedEdition(edition + 4L))) {
							continue;
						}
						if (blockedKeys.isBlocked(usk.setSuggestedEdition(edition + 5L))) {
							continue;
						}
						if (keysInIndex.isReplaced(usk) || keysInIndex.contains(usk)) {
							// This term can be removed since there is an USK
							// with a newer page in the index.
							// This is a somewhat half-hearted logic. For pages
							// not updated to many times between the SSK was
							// entered in the index and the USK is found, this
							// is fine.
							// For pages updated, the SSK will start to be
							// removed before the replacement is entirely in 
							// the index.
							// On the other hand, SSKs will not be maintained
							// in the index.
							writeTermEntry(new TermDeletePageEntry(tpe));
							++countWrittenEntriesThisTerm;
						}
					}
				}
			}
			if (set.size() == 0) {
			    emptyTerms++;
			}
			System.out.print("                                ");
			System.out.print("                                ");
			System.out.print("                                \r");
			if (countWrittenEntriesThisTerm > 0) {
			    System.out.println("" + termNumber + " " +
					       term + " " +
					       countWrittenEntriesThisTerm + "/" +
					       set.size());
			}
			set.deflate();

			// Do one file full of removals at the time.
			if (openedFile != null && openedFile.isFullDeletionsFile()) {
				openedFile.close();
				openedFile = null;

				rotateFile();
			}
		}
		if (openedFile != null) {
			openedFile.close();
			openedFile = null;

			rotateFile();
		}
		keysInIndex.flush();
		System.out.println("Filled " + countFilledFiles + " files.");
		if (emptyTerms > 0) {
		    System.out.println("There were " + emptyTerms +
				       " terms without entries.");
		}
		if (termOverflowCount > 0) {
		    System.out.println("" + termOverflowCount + " terms were not fully deleted.");
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
