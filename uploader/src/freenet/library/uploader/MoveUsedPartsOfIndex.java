/*
 */

/*
 * Log levels used:
 * None/Warning: Serious events and small problems.
 * FINE: Stats for fetches and overview of contents of fetched keys. Minor events.
 * FINER: Queue additions, length, ETA, rotations.
 * FINEST: Really minor events.
 */

package freenet.library.uploader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.LinkedBlockingDeque;

import freenet.library.io.FreenetURI;

/**
 * Class to download the entire index.
 */
class MoveUsedPartsOfIndex extends AdHocDataReader {
	private FreenetURI uri;
	private LinkedBlockingDeque<Page> objectQueue =
		new LinkedBlockingDeque<Page>();

	public MoveUsedPartsOfIndex(FreenetURI u) {
		uri = u;
	}


	/**
	 * A class to keep track of what pages are fetched and how they are related
	 * to other fetched pages. The purpose of this is to avoid fetching stuff
	 * related only to "old" editions.
	 */
	private static class Page {
		private Set<Page> children = Collections.synchronizedSet(new HashSet<Page>());

		private FreenetURI uri;
		int level;

		Page(FreenetURI u) {
			this(u, 0);
		}

		Page(FreenetURI u, int l) {
			uri = u;
			level = l;
		}

		void addChild(Page fp) {
			children.add(fp);
		}

		Page newChild(FreenetURI u) {
			Page child = new Page(u, level + 1);
			addChild(child);
			return child;
		}

		FreenetURI getURI() {
			return uri;
		}
	}

	/**
	 * 1. chdir to the directory with all the files.
	 * 2. Give parameters CHK/filename
	 * The CHK/filename is of the top file (stated in library.index.lastpushed.chk).
	 */
	public void doMove() {
		int count = 0;
		File toDirectory = new File("../" + UploaderPaths.LIBRARY_CACHE + ".new");
		if (!toDirectory.mkdir()) {
			System.err.println("Could not create the directory " + toDirectory);
			System.exit(1);
		}
		final Page fetchedPage = new Page(uri);
		objectQueue.add(fetchedPage);
		while (objectQueue.size() > 0) {
			Page page;
			try {
				page = objectQueue.takeLast();
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				System.exit(1);
				return;
			}
			final Page finalPage = page;
			FileInputStream inputStream;
			try {
				Files.createLink(Paths.get(toDirectory.getPath(), page.uri.toString()), Paths.get(page.uri.toString()));
				inputStream = new FileInputStream(page.uri.toString());
				count++;
				System.out.println("Read " + count + " files. " +
						"Still queued: " + objectQueue.size() + ". " +
						"URI " + page.uri + " at level " + page.level);
			} catch (IOException e) {
				System.out.println("Cannot find file " + page.uri);
				e.printStackTrace();
				System.exit(1);
				return;
			}
			try {
				readAndProcessYamlData(inputStream,
						new UriProcessor() {
					@Override
					public FreenetURI getURI() {
						return finalPage.getURI();
					}

					@Override
					public int getLevel() {
						return 1;
					}

					Set<FreenetURI> seen = new HashSet<FreenetURI>();
					@Override
					public boolean processUri(FreenetURI uri) {
						if (seen.contains(uri)) {
							return false;
						}
						seen.add(uri);
						objectQueue.offer(finalPage.newChild(uri));
						return true;
					}

					@Override
					public void uriSeen() {}

					@Override
					public void stringSeen() {}

					@Override
					public void childrenSeen(int level, int foundChildren) {}

				}, page.level);
			} catch (IOException e) {
				System.out.println("Cannot read file " + page.uri);
				e.printStackTrace();
				System.exit(1);
				return;
			}
		}
	}

	public static void main(String[] argv) {
		if (argv.length > 0) {
			try {
				new MoveUsedPartsOfIndex(new FreenetURI(argv[0])).doMove();
			} catch (MalformedURLException e) {
				e.printStackTrace();
				System.exit(2);
			}
		}
	}
}
