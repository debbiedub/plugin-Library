/*
 */

package freenet.library.uploader;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.FileAlreadyExistsException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Formatter;
import java.util.HashSet;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import freenet.library.io.FreenetURI;
import net.pterodactylus.fcp.AllData;
import net.pterodactylus.fcp.ClientGet;
import net.pterodactylus.fcp.ClientPut;
import net.pterodactylus.fcp.FcpAdapter;
import net.pterodactylus.fcp.FcpConnection;
import net.pterodactylus.fcp.GetFailed;
import net.pterodactylus.fcp.Priority;
import net.pterodactylus.fcp.PutFailed;
import net.pterodactylus.fcp.PutSuccessful;
import net.pterodactylus.fcp.SubscribeUSK;
import net.pterodactylus.fcp.SubscribedUSKUpdate;
import net.pterodactylus.fcp.URIGenerated;
import net.pterodactylus.fcp.Verbosity;

/**
 * Class to download the entire index and then keep downloading and
 * uploading missing parts.
 *
 * When a newer USK is seen, stop the processing immediately and exit.
 *
 * The logic is:
 * * Download the USK and then the parts referenced from it (currently in 
 *   5 levels).
 * * If any of the parts referenced is already downloaded, put it in the
 *   refetch (download again) queue.
 * * If a non-downloadable part is encountered attempt to download later.
 *   This is the fetchunfetchable queue. Attempt each page with a growing
 *   minimum time since the previous attempt.
 * * When all parts of the index is downloaded, download the same pages
 *   again from the refetch queue. Refetch each page with a growing minimum
 *   time since the previous attempt. If not downloadable, upload it from
 *   the saved copy.
 * * When all parts of the index are attempted, remove the parts on disk
 *   that were not referenced from this USK. They are referenced
 *   from previous USKs and will never be used again.
 * * If running this with access to the complete list of uploaded parts,
 *   the fetchunfetchable queue can also be processed by finding the files
 *   from the upload store and uploading them.
 * * Always find a random part from the queues. This allows any number of
 *   this program to run on different nodes with the best possible spread 
 *   of downloads and uploads over the whole index.
 * * The amount of fetches and retrievals are balanced with the ambition to 
 *   have the amount of fetches and uploads running reflecting the relative
 *   size of the queues.
 *   The refetch queue is excluded from this calculation since it always
 *   considered last with the ideal situation that it contains the whole
 *   of the index.
 *
 * Log levels used:
 * None/Warning: Serious events and small problems.
 * FINE: Stats for fetches and overview of contents of fetched keys. Minor events.
 * FINER: Queue additions, length, ETA, rotations.
 * FINEST: Really minor events.
 *
 * Conventions:
 * * do*() methods accept a page, do something and deliver the page to a queue (depending on the result).
 * * doHandle*() methods do very little before delivering to a queue.
 */
class DownloadOneEdition {
	/** Logger. */
	private static final Logger logger = Logger.getLogger(DownloadOneEdition.class.getName());

	/**
	 * Thread pool for executors that are mostly waiting for Freenet operations
	 * (fetching and storing).
	 *
	 * For #run2(), the size of the pool is used as the limit of the amount of
	 * concurrent operations against Freenet. This is done by scheduling an executor
	 * to scheduling more operations.
	 */
	private ScheduledExecutorService FCPexecutors;

	/**
	 * Thread pool for executors that are not doing Freenet operations.
	 *
	 * Mostly idle except when busy reading and parsing pages.
	 */
	private ScheduledExecutorService otherExecutors;

	private FcpConnection connection;
	private boolean closingDown = false;
	private File directory;
	private File morePagesDirectory;
	private CleanupOldFiles cleanUp = null;
	private Unfetchables unfetchables = new Unfetchables();
	private int getterCounter = 0;
	private int uploadCounter = 0;

	private AdHocDataReader reader = new AdHocDataReader();

	private static final long OPERATION_GIVE_UP_TIME = TimeUnit.HOURS.toMillis(2);
	private static final long FREE_SPACE_LEFT_LIMIT = 1024L * 1024L * 1024L;

	/**
	 * Keep track of if we are below or at the threshold or not.
	 *
	 * If this is less than half of the threshold, this will be two.
	 */
	private long timesBelowSpaceThreshold = 0L;

	class RotatingQueue<E> extends LinkedBlockingQueue<E> {
		/**
		 * Serializeable.
		 */
		private static final long serialVersionUID = -9157586651059771247L;

		public RotatingQueue(Random r) {
			random = r;
		}

		@Override
		public E poll() {
			int toRotate = 0;
			int s = size();
			if (s > 0) {
				toRotate = random.nextInt(s);
			}
			while (true) {
				E taken;

				taken = super.poll();
				if (taken == null) {
					return null;
				}
				if (--toRotate > 0) {
					offer(taken); // Ignoring impossible false status.
					continue;
				}
				return taken;
			}
		}

		@Override
		public E poll(long l, TimeUnit u) {
			throw new IllegalStateException("Not implemented");
		}

		@Override
		public E take() throws InterruptedException {
			E taken = poll();
			if (taken == null) {
				return super.take();
			}
			return taken;
		}
	}


	/**
	 * A class to keep track of the Pages we work with.
	 */
	private class Page {
		private final long START_DEFER_TIME = TimeUnit.MINUTES.toMillis(30);
		private final long RANGE_ADD_DEFER_TIME = TimeUnit.MINUTES.toMillis(10);

		private FreenetURI uri;
		private int level = 0;
		Date nextFetchAttempt = new Date();
		StringBuffer logAttempts;
		private long timeToNextFetchAttempt;

		Page(FreenetURI u, Page p) {
			uri = u;
			if (p != null) {
				level = p.level + 1;
			}
			nextFetchAttempt = new Date();
			logAttempts = new StringBuffer();
			timeToNextFetchAttempt = START_DEFER_TIME;
		}

		FreenetURI getURI() {
			return uri;
		}

		int getLevel() {
			return level;
		}

		File getFile() {
			return new File(directory, getFileBaseName());
		}

		File getGZipFile() {
			return new File(directory, getFileBaseName() + ".gz");
		}

		/**
		 * Return existing file storing the page.
		 * 
		 * If there is no such file, return any file that could store the page.
		 */
		File getAnyFile() {
			File file = getGZipFile();
			if (!file.exists()) {
				return getFile();
			}
			return file;
		}

		/**
		 * @return the length of the file.
		 */
		public long getFileLength() {
			File file = getFile();
			if (file.exists()) {
				return getFile().length();
			}
			InputStream is = null;
			try {
				is = getInputStream();
				int s = 0;
				while (is.read() != -1) {
					s++;
				}
				return s;
			} catch (IOException e) {
				// Ignore this problem. It shouldn't happen.
				return 0;
			} finally {
				if (is != null) {
					try {
						is.close();
					} catch (IOException e) {
						// Ignore this problem. It shouldn't happen.
					}
				}
			}
		}

		private String getFileBaseName() {
			return getURI().toString().replace("/", "__");
		}

		void deleteFile() {
			getGZipFile().delete();
			getFile().delete();
		}

		InputStream getInputStream() throws IOException {
			try {
				return new GZIPInputStream(new FileInputStream(getGZipFile()));
			} catch (FileNotFoundException e) {
				return new FileInputStream(new File(directory, getFileBaseName()));
			}
		}

		OutputStream getOutputStream() throws FileNotFoundException, IOException {
			File file = getGZipFile();
			return new GZIPOutputStream(new FileOutputStream(file));
		}

		private void calculateNextFetchAttempt() {
			nextFetchAttempt = new Date(new Date().getTime() + timeToNextFetchAttempt);
		}

		/**
		 * Calculate the time until the next attempt to fetch this page.
		 *
		 * The timeToNextFetchAttempt is increased by a random number between 0 and
		 * RANGE_ADD_DEFER_TIME. Increasing the time will eventually reduce the
		 * frequency of fetching unfetchables in favor of re-fetching.
		 */
		void deferFetchAgain() {
			timeToNextFetchAttempt += random.nextInt(Long.valueOf(RANGE_ADD_DEFER_TIME).intValue());
			calculateNextFetchAttempt();
			logAttempts.append("Deferring at ").append(new Date()).append(" until ").append(nextFetchAttempt).append("\n");
		}

		boolean fetchAvailable() {
			return new Date().after(nextFetchAttempt);
		}

		/**
		 * Reset counting for calculation of the time until the next attempt to
		 * fetch this page.
		 *
		 * This should normally be called when changing queue.
		 */
		void deferFetchReset() {
			timeToNextFetchAttempt = START_DEFER_TIME;
			logAttempts = new StringBuffer();
			logAttempts.append("Deferred to ").append(new Date()).append("\n");
			calculateNextFetchAttempt();
		}
	}

	class AvoidRecentFetchesQueue extends RotatingQueue<Page> {
		/**
		 * Serializeable.
		 */
		private static final long serialVersionUID = 7608442014226987011L;

		AvoidRecentFetchesQueue(Random r) {
			super(r);
		}

		public Page pollNotDeferred() {
			int maxLaps = size();
			if (maxLaps > 10) {
				maxLaps = 6;
			}
			do {
				Page page = poll();
				if (page == null) {
					return page;
				}
				if (page.fetchAvailable()) {
					return page;
				}
				logger.finest("Skipped page deferred until " + page.nextFetchAttempt);
				offer(page); // Ignored impossible false status
			} while (--maxLaps > 0);
			logger.finest("Did not find any not deferred page");
			return null;
		}
	}



	private Random random = new Random();
	private RotatingQueue<Page> toParse = new RotatingQueue<Page>(random);
	/**
	 * Pages to be fetched for the first time.
	 */
	private RotatingQueue<Page> toFetch = new RotatingQueue<Page>(random);
	/**
	 * Pages that were not possible to fetch.
	 */
	private AvoidRecentFetchesQueue toFetchUnfetchable = new AvoidRecentFetchesQueue(random);
	/**
	 * Pages that have been successfully fetched.
	 */
	private AvoidRecentFetchesQueue toRefetch = new AvoidRecentFetchesQueue(random);
	private AvoidRecentFetchesQueue toUploadUnfetchable = new AvoidRecentFetchesQueue(random);

	private int counterParseSuccess = 0;
	private int counterParseFailed = 0;
	private int counterFetchSuccess = 0;
	private int counterFetchFailed = 0;
	private int counterFetchUnfetchableSuccess = 0;
	private int counterFetchUnfetchableFailed = 0;
	private int counterCopyUploadUnfetchableSuccess = 0;
	private int counterCopyUploadUnfetchableFailed = 0;
	private int counterRefetchSuccess = 0;
	private int counterRefetchFailed = 0;
	private int counterUploadUnfetchableSuccess = 0;
	private int counterUploadUnfetchableFailed = 0;
	private int counterRefetchUploadSuccess = 0;
	private int counterRefetchUploadFailed = 0;
	private int counterRemovedBecauseOfNotEnoughFreeSpaceLeft = 0;

	private int edition;

	private static String STATISTICS_FORMAT_PREFIX = "%-24s%7d%7d%7d";

	public synchronized final void logStatistics() {
		StringBuffer sb = new StringBuffer();
		sb.append(statisticsLine("toParse", counterParseSuccess, counterParseFailed, toParse));
		sb.append(statisticsLine("toFetch", counterFetchSuccess, counterFetchFailed, toFetch));
		sb.append(statisticsLine("toFetchUnfetchable",
					 counterFetchUnfetchableSuccess, counterFetchUnfetchableFailed,
					 toFetchUnfetchable));
		int counterCopyUploadUnfetchable = counterCopyUploadUnfetchableSuccess + counterCopyUploadUnfetchableFailed;
		if (counterCopyUploadUnfetchable > 0) {
			sb.append(new Formatter().format(STATISTICS_FORMAT_PREFIX,
							 "CopyUploadUnfetchable",
							 counterCopyUploadUnfetchable,
							 counterCopyUploadUnfetchableSuccess,
							 counterCopyUploadUnfetchableFailed)).append("\n");
		}
		sb.append(statisticsLine("toRefetch",
					 counterRefetchSuccess, counterRefetchFailed,
					 toRefetch));
		sb.append(statisticsLine("toUploadUnfetchable",
					 counterUploadUnfetchableSuccess, counterUploadUnfetchableFailed,
					 toUploadUnfetchable));
		int counterRefetchUpload = counterRefetchUploadSuccess + counterRefetchUploadFailed;
		if (counterRefetchUpload > 0) {
			sb.append(new Formatter().format(STATISTICS_FORMAT_PREFIX,
							 "RefetchUploadUnfetchable",
							 counterRefetchUpload,
							 counterRefetchUploadSuccess,
							 counterRefetchUploadFailed)).append("\n");
		}
		if (cleanUp != null) {
			cleanUp.addLog(sb);
		}
		if (unfetchables.size() > 0) {
			sb.append("Unfetchables from previous run: " + unfetchables.size() + "\n");
		}
		if (counterRemovedBecauseOfNotEnoughFreeSpaceLeft > 0) {
			sb.append("Removed from cache because not enough free space: " + counterRemovedBecauseOfNotEnoughFreeSpaceLeft + "\n");
		}
		logger.info("Statistics for " + edition + ":\n" + sb.toString() + "End Statistics.");
	}

	private static String STATISTICS_FORMAT = STATISTICS_FORMAT_PREFIX + "%6d%3d%3d%5d%6d%6d%6d\n";

	public final String statisticsLine(String r, int success, int failed, RotatingQueue<Page> rqp) {
		int counter = success + failed;
		if (rqp.size() > 0 || counter > 0) {
			int arr[] = new int[12];
			for (Page p : rqp) {
				arr[p.level]++;
			}
			Formatter formatter = new Formatter();
			String line = formatter.format(STATISTICS_FORMAT, r, counter, success, failed,
							rqp.size(), arr[0], arr[1], arr[2], arr[3], arr[4], arr[5])
					.toString();
			formatter.close();
			return line;
		}
		return "";
	}

	private void copy(InputStream source, OutputStream sink) throws IOException {
		try (OutputStream out = new BufferedOutputStream(sink);
				InputStream in   = new BufferedInputStream(source)) {
			int ch;
			while ((ch = in.read()) != -1) {
				out.write(ch);
			}
		}
	}

	private boolean fetch(final Page page) {
		int counter;
		synchronized (this) {
			counter = ++getterCounter;
		}
		final String token = "Getter" + counter;
		final ClientGet getter = new ClientGet(page.getURI().toString(), token);
		getter.setPriority(Priority.prefetch);
		getter.setVerbosity(Verbosity.NONE);
		final boolean[] results = new boolean[1];
		results[0] = false;
		FcpAdapter listener = new FcpAdapter() {
			@Override
			public void receivedAllData(FcpConnection c, AllData ad) {
				assert c == connection;
				assert ad != null;
				if (!token.equals(ad.getIdentifier())) {
					return;
				}
				logger.entering(DownloadOneEdition.class.toString(), "receivedAllData", "receivedAllData for " + token);
				try {
					copy(ad.getPayloadInputStream(), page.getOutputStream());
				} catch (IOException ioe) {
					page.deleteFile();
					synchronized (getter) {
						getter.notify();
					}
					return;
				}
				results[0] = true;
				synchronized (getter) {
					getter.notify();
				}
			}

			@Override
			public void receivedGetFailed(FcpConnection c, GetFailed gf) {
				assert c == connection;
				assert gf != null;
				if (!token.equals(gf.getIdentifier())) {
					return;
				}
				synchronized (getter) {
					getter.notify();
				}
				logger.fine("receivedGetFailed for " + token + " (" + page.getURI() + ").");
			}

			@Override
			public void receivedSimpleProgress(FcpConnection c, net.pterodactylus.fcp.SimpleProgress sp) {
				assert c == connection;
				assert sp != null;
				if (!token.equals(sp.getIdentifier())) {
					return;
				}
				logger.finest("Progress for " + token + " (" + sp.getSucceeded() + "/" + sp.getRequired() + "/"
						+ sp.getTotal() + ").");
			}
		};
		connection.addFcpListener(listener);
		try {
			connection.sendMessage(getter);
		} catch (IOException e) {
			logger.log(Level.SEVERE, "Exception", e);
			return false;
		}
		synchronized (getter) {
			try {
				getter.wait(OPERATION_GIVE_UP_TIME);
			} catch (InterruptedException e) {
				if (!closingDown) {
					logger.log(Level.SEVERE, "Exception", e);
				}
				return false;
			}
		}
		connection.removeFcpListener(listener);

		boolean result = results[0];
		page.deferFetchAgain();

		return result;
	}

	private void parse(final Page page) {
		try {
			reader.readAndProcessYamlData(page.getInputStream(), new AdHocDataReader.UriProcessor() {
				@Override
				public FreenetURI getURI() {
					return page.getURI();
				}

				@Override
				public int getLevel() {
					return page.getLevel();
				}

				Set<FreenetURI> seen = new HashSet<FreenetURI>();

				@Override
				public boolean processUri(FreenetURI uri) {
					if (seen.contains(uri)) {
						return false;
					}
					seen.add(uri);
					doHandleNew(new Page(uri, page));
					return true;
				}

				@Override
				public void uriSeen() {
				}

				@Override
				public void stringSeen() {
				}

				@Override
				public void childrenSeen(int level, int foundChildren) {
				}


			}, page.getLevel());
		} catch (IOException ioe) {
			page.deleteFile();
		}
	}

	private boolean upload(final Page page) {
		final boolean[] successfuls = new boolean[1];
		successfuls[0] = false;
		int counter;
		synchronized (this) {
			counter = ++uploadCounter;
		}
		final String identifier = "Upload" + counter;
		final ClientPut putter = new ClientPut("CHK@", identifier);
		putter.setEarlyEncode(true);
		putter.setPriority(net.pterodactylus.fcp.Priority.bulkSplitfile);
		putter.setVerbosity(Verbosity.NONE);
		final long dataLength = page.getFileLength();
		putter.setDataLength(dataLength);

		final FcpAdapter listener = new FcpAdapter() {
			@Override
			public void receivedURIGenerated(FcpConnection c, URIGenerated uriGenerated) {
				assert c == connection;
				assert uriGenerated != null;
				if (!identifier.equals(uriGenerated.getIdentifier())) {
					return;
				}
				FreenetURI chk = page.getURI();
				FreenetURI generatedURI;
				try {
					generatedURI = new FreenetURI(uriGenerated.getURI());
				} catch (MalformedURLException e) {
					logger.severe(
							"Were supposed to resurrect " + chk + " but the URI calculated to " + uriGenerated.getURI()
									+ " that is not possible to convert to an URI. Will upload anyway.");
					return;
				}
				if (!generatedURI.equals(chk)) {
					logger.severe("Were supposed to resurrect " + chk + " but the URI calculated to "
							+ uriGenerated.getURI() + ". " + "Will upload anyway.");
				} else {
					logger.finest("Resurrecting " + chk);
				}
			}

			@Override
			public void receivedPutSuccessful(FcpConnection c, PutSuccessful putSuccessful) {
				assert c == connection;
				assert putSuccessful != null;
				if (!identifier.equals(putSuccessful.getIdentifier())) {
					return;
				}
				FreenetURI chk = page.getURI();
				FreenetURI generatedURI = null;
				try {
					try {
						generatedURI = new FreenetURI(putSuccessful.getURI());
					} catch (MalformedURLException e) {
						logger.severe("Uploaded " + putSuccessful.getURI() + " that is not possible to convert to an URI.");
						return;
					}
					if (!generatedURI.equals(chk)) {
						logger.severe("Uploaded " + putSuccessful.getURI() + " while supposed to upload " + chk + ". ");
						return;
					}
					logger.finer("Uploaded " + chk);
					successfuls[0] = true;
				} finally {
					synchronized (putter) {
						putter.notify();
					}
				}
			}

			@Override
			public void receivedPutFailed(FcpConnection c, PutFailed putFailed) {
				assert c == connection;
				assert putFailed != null;
				if (!identifier.equals(putFailed.getIdentifier())) {
					return;
				}
				FreenetURI chk = page.getURI();
				logger.severe("Uploaded " + chk + " failed.");
				synchronized (putter) {
					putter.notify();
				}
			}
		};
		connection.addFcpListener(listener);
		InputStream in;
		try {
			in = page.getInputStream();
			putter.setPayloadInputStream(in);
			connection.sendMessage(putter);
			synchronized (putter) {
				putter.wait(OPERATION_GIVE_UP_TIME);
			}
			in.close();
			in = null;
		} catch (IOException | NullPointerException e) {
			logger.log(Level.WARNING, "Upload failed for " + page, e);
		} catch (InterruptedException e) {
			if (!closingDown) {
				logger.log(Level.WARNING, "Upload interrupted for " + page, e);
			}
			return false;
		} finally {
			connection.removeFcpListener(listener);
		}
		return successfuls[0];
	}

	private boolean doFetchUnfetchable(Page page) {
		boolean result = fetch(page);
		if (result) {
			page.deferFetchReset();
			toParse.offer(page);
			counterFetchUnfetchableSuccess++;
		} else {
			toFetchUnfetchable.offer(page);
			counterFetchUnfetchableFailed++;
		}
		return result;
	}

	private boolean doRefetchToUpload(Page page) {
		boolean result = fetch(page);
		if (result) {
			page.deferFetchReset();
			toRefetch.offer(page);
			counterRefetchUploadSuccess++;
		} else {
			toUploadUnfetchable.offer(page);
			counterRefetchUploadFailed++;
		}
		return result;
	}

	private boolean doRefetch(Page page) {
		boolean result = fetch(page);
		if (result) {
			toRefetch.offer(page);
			counterRefetchSuccess++;
			// If there is not enough room left we will remove the recently
			// fetched file. If we didn't manage to fetch it, it will be
			// queued for upload.
			timesBelowSpaceThreshold = FREE_SPACE_LEFT_LIMIT / page.getAnyFile().getFreeSpace(); 
			if (timesBelowSpaceThreshold > 0) {
				logger.finest("Removing file after refetch " + page.getAnyFile());
				page.deleteFile();
				counterRemovedBecauseOfNotEnoughFreeSpaceLeft++;
			}
		} else {
			if (page.getAnyFile().exists()) {
				page.deferFetchReset();
				toUploadUnfetchable.offer(page);
			} else {
				toRefetch.offer(page);
			}
			counterRefetchFailed++;
		}
		return result;
	}

	/**
	 * A reference to a new page is seen for the first time.
	 * @param page
	 */
	private void doHandleNew(Page page) {
		File file = page.getAnyFile();
		if (file.exists()) {
			file.setLastModified(System.currentTimeMillis());
			if (cleanUp != null) {
				cleanUp.remove(file);
			}
			toParse.offer(page);
		} else if (unfetchables.remove(page.getURI())) {
			toFetchUnfetchable.offer(page);
		} else {
			toFetch.offer(page);
		}
	}

	private boolean doFetch(Page page) {
		boolean result = fetch(page);
		page.deferFetchReset();
		if (result) {
			toParse.offer(page);
			counterFetchSuccess++;
		} else {
			toFetchUnfetchable.offer(page);
			counterFetchFailed++;
		}
		return result;
	}

	private void doParse(Page page) {
		parse(page);
		counterParseSuccess++;
		if (unfetchables.remove(page.getURI())) {
			toUploadUnfetchable.offer(page);
		} else {
			toRefetch.offer(page);
		}
	}

	private boolean doUploadUnfetchable(Page page) {
		boolean result = upload(page);
		toRefetch.offer(page);
		if (result) {
			counterUploadUnfetchableSuccess++;
		} else {
			counterUploadUnfetchableFailed++;
		}
		return result;
	}

	private void doCopyAndUploadUnfetchable(Page page) {
		File fromFile = new File(morePagesDirectory, page.getFile().getName());
		try {
			Files.copy(fromFile.toPath(), page.getFile().toPath());
			boolean result = upload(page);
			if (result) {
				toParse.offer(page);
				counterCopyUploadUnfetchableSuccess++;
			} else {
				toFetchUnfetchable.offer(page);
				counterCopyUploadUnfetchableFailed++;
			}
			logger.finer("Uploaded Unfetchable" + (result ? "" : "failed") + ".");
		} catch (UnsupportedOperationException uoe) {
			logger.log(Level.SEVERE, "Could not copy file " + fromFile + " to " + page.getFile() + ".", uoe);
			toFetchUnfetchable.offer(page);
		} catch (FileAlreadyExistsException faee) {
			logger.log(Level.SEVERE, "Could not copy file " + fromFile + " to " + page.getFile() + ".", faee);
			toUploadUnfetchable.offer(page);
		} catch (IOException ioe) {
			logger.log(Level.SEVERE, "Could not copy file " + fromFile + " to " + page.getFile() + ".", ioe);
			if (page.getAnyFile().exists()) {
				page.deleteFile();
				logger.info("Deleted partial copy " + page);
			}
			toFetchUnfetchable.offer(page);
		} catch (SecurityException se) {
			logger.log(Level.SEVERE, "Could not copy file " + fromFile + " to " + page.getFile() + ".", se);
			toFetchUnfetchable.offer(page);
		}
	}

	private class CleanupOldFiles implements Runnable {
		private final Set<File> allFiles = Collections.synchronizedSet(new HashSet<File>());
		private ScheduledFuture<?> handle = null;
		private int count = 1;

		public CleanupOldFiles() {
			allFiles.addAll(Arrays.asList(directory.listFiles()));
		}

		public ScheduledFuture<?> setHandle(ScheduledFuture<?> h) {
			handle = h;
			return h;
		}

		public void addLog(StringBuffer sb) {
			if (allFiles.size() > 0) {
				sb.append("Files left to remove: " + allFiles.size() + "\n");
			}
		}

		public void remove(File f) {
			allFiles.remove(f);
		}

		public void run() {
			if (allFiles.size() == 0) {
				if (handle != null) {
					handle.cancel(true);
					handle = null;
				}
				return;
			}
			File anyFile = null;
			synchronized(allFiles) {
				for (File f : allFiles) {
					anyFile = f;
					break;
				}
			}
			if (anyFile.getFreeSpace() > FREE_SPACE_LEFT_LIMIT) {
				if (toParse.size() > 0) {
					logger.finest("Don't delete anything, the parsing is not completed.");
					count = 1;
					return;
				}
				if (toFetch.size() > 0) {
					logger.finer("Don't delete anything, the fetching is not completed.");
					count = 1;
					return;
				}
				if (toRefetch.size() == 0) {
					logger.finer("Don't delete anything, the fetching has not started.");
					count = 1;
					return;
				}
			} else {
				count += 1;
				logger.finer("Will delete " + count + " files since free space limit is exceeded.");
			}
			try {
				// Sort in oldest order.
				SortedSet<File> toRemove = new TreeSet<File>(new Comparator<File>() {
					@Override
					public int compare(File o1, File o2) {
						int l = Long.compare(o1.lastModified(), o2.lastModified());
						if (l != 0) {
							return l;
						}
						return o1.getName().compareTo(o2.getName());
					}
				});
				synchronized(allFiles) {
					for (File f : allFiles) {
						toRemove.add(f);
						if (toRemove.size() > count) {
							toRemove.remove(toRemove.last());
						}
					}
				}
				for (File f : toRemove) {
					allFiles.remove(f);
					try {
						unfetchables.remove(new FreenetURI(f.getName()));
					} catch (MalformedURLException e) {
						logger.log(Level.WARNING, "File " + f + " strange filename.", e);
					}
					if (f.exists()) {
						logger.finest("Removing file " + f);
						f.delete();
					}
				}
				count += 1 + count / 7;
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Uncaught exception", e);
				throw e;
			}
		}
	}

	/**
	 * Class to keep track of uploads from the previous run.
	 */
	private static class Unfetchables {
		private Set<FreenetURI> fromPreviousRun = new HashSet<FreenetURI>();
		private final static String UNFETCHABLES_FILENAME = "unfetchables.saved";
		private File directory;

		@SuppressWarnings("unchecked")
		private Set<FreenetURI> extracted(ObjectInputStream ois) throws IOException, ClassNotFoundException {
			return (Set<FreenetURI>) ois.readObject();
		}

		void load(File dir) {
			directory = dir;
			File file = new File(directory, UNFETCHABLES_FILENAME);
			if (file.exists()) {
				logger.finest("Reading file " + file);
				try {
					FileInputStream f = new FileInputStream(file);
					ObjectInputStream ois = new ObjectInputStream(f);
					fromPreviousRun = extracted(ois);
					ois.close();
				} catch (IOException e) {
					logger.warning("Could not read the file " + file);
				} catch (ClassCastException | ClassNotFoundException e) {
					logger.warning("File " + file + " contains strange object");
				} finally {
					file.delete();
				}
			} else {
				logger.finest("No file " + file);
			}
		}

		private void rotate() {
			String OLD_FILENAME = UNFETCHABLES_FILENAME + ".old";
			File oldfile = new File(directory, OLD_FILENAME);
			if (oldfile.exists()) {
				oldfile.delete();
			}
			File file = new File(directory, UNFETCHABLES_FILENAME);
			if (file.exists()) {
				file.renameTo(oldfile);
			}
		}

		synchronized void save(RotatingQueue<Page> toSave1, RotatingQueue<Page> toSave2) {
			rotate();
			File file = new File(directory, UNFETCHABLES_FILENAME);
			Set<FreenetURI> set = new HashSet<FreenetURI>();
			for (Page p : toSave1) {
				set.add(p.getURI());
			}
			if (toSave2 != null) {
				for (Page p : toSave2) {
					set.add(p.getURI());
				}
			}
			if (set.size() > 0) {
				logger.finest("Writing file " + file);
				try {
					FileOutputStream f = new FileOutputStream(file);
					ObjectOutputStream oos = new ObjectOutputStream(f);
					oos.writeObject(set);
					oos.close();
					f.close();
				} catch (IOException e) {
					logger.log(Level.WARNING, "Problem writing file " + file, e);
					file.delete();
				}
			} else {
				logger.finest("Nothing to write to file " + file);
			}
		}

		synchronized int size() {
			return fromPreviousRun.size();
		}

		/**
		 * @param uri The uri to remove.
		 * @return true if the uri was among the unfetchables.
		 */
		synchronized boolean remove(FreenetURI uri) {
			return fromPreviousRun.remove(uri);
		}
	}

	private abstract class ProcessSomething implements Runnable {
		protected abstract void process();

		public void run() {
			try {
				process();
			} catch (RejectedExecutionException e) {
				// Do nothing.
				if (!closingDown) {
					logger.log(Level.SEVERE, "Confusion in the executor or queue full.", e);
				}
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Class " + this + " threw exception: " + e, e);
			}
		}
	}

	private class ProcessParse extends ProcessSomething {
		protected void process() {
			Page page = toParse.poll();
			if (page != null) {
				doParse(page);
				otherExecutors.schedule(this, 200, TimeUnit.MILLISECONDS);
			} else {
				otherExecutors.schedule(this, 10, TimeUnit.SECONDS);
			}
		}
	}

	private class ProcessUploadUnfetchable extends ProcessSomething {
		protected void process() {
			if (morePagesDirectory != null) {
				Page page = toFetchUnfetchable.poll();
				if (page != null) {
					doCopyAndUploadUnfetchable(page);
					return;
				}
			}

			Page page = toUploadUnfetchable.poll();
			if (page != null) {
				boolean result = doUploadUnfetchable(page);
				logger.finer("Uploaded Unfetchable" + (result ? "" : "failed") + ".");
				return;
			}
		}
	}

	/**
	 * This is the bulk of all fetches.
	 *
	 * Mostly Fetch, if any, but sometimes one of the refetches.
	 */
	private class ProcessFetches extends ProcessSomething {
		protected void process() {
			Page page = toFetch.poll();
			if (page != null) {
				boolean result = doFetch(page);
				logger.finest("Fetched Fetch" + (result ? "" : " failed") + ".");
				return;
			}

			page = toFetchUnfetchable.pollNotDeferred();
			if (page != null) {
				String log = page.logAttempts.toString();
				boolean result = doFetchUnfetchable(page);
				logger.finer(log + "Fetched RefetchUnfetchable" + (result ? "" : " failed") + ".");
				return;
			}

			page = toUploadUnfetchable.pollNotDeferred();
			if (page != null) {
				String log = page.logAttempts.toString();
				boolean result = doRefetchToUpload(page);
				logger.finer(log + "Fetched ToUpload" + (result ? "" : " failed") + ".");
				return;
			}

			page = toRefetch.pollNotDeferred();
			if (page != null) {
				boolean result = doRefetch(page);
				logger.finer("Fetched Refetch" + (result ? "" : " failed") + ".");
				return;
			}
		}
	}

	void shutdown(FcpSession session) {
		waitTermination(TimeUnit.SECONDS.toMillis(1));
		closingDown = true;
		logger.info("Shutdown.");
		FCPexecutors.shutdown();
		otherExecutors.shutdown();
		unfetchables.save(toUploadUnfetchable, toFetchUnfetchable);
		if (!waitTermination(TimeUnit.MINUTES.toMillis(1) + OPERATION_GIVE_UP_TIME)) {
			logger.info("Shutdown now (after long wait).");
			FCPexecutors.shutdownNow();
			otherExecutors.shutdownNow();
			session.close();
			if (!waitTermination(TimeUnit.MINUTES.toMillis(1))) {
				logger.info("Shutdown now did not succeed to stop all jobs");
			}
		}
		FCPexecutors = null;
		otherExecutors = null;
		session.close();
		session = null;
		logger.info("Shutdown completed.");
	}

	private void run(FreenetURI u, File morePagesDir) {
		morePagesDirectory = morePagesDir;
		FCPexecutors = Executors.newScheduledThreadPool(10);
		otherExecutors = Executors.newScheduledThreadPool(1);
		directory = new File("library-download-all-once-db");
		if (directory.exists()) {
			unfetchables.load(directory);
			cleanUp = new CleanupOldFiles();
			cleanUp.setHandle(otherExecutors.scheduleWithFixedDelay(cleanUp, 500, 1, TimeUnit.MINUTES));
		} else {
			directory.mkdir();
		}

		otherExecutors.scheduleAtFixedRate(new Runnable() {
			public void run() {
				logStatistics();
			}
		}, 10, 30, TimeUnit.SECONDS);
		for (int i = 0; i < 10; i++) {
			FCPexecutors.scheduleWithFixedDelay(new ProcessFetches(), 20 + i, 4, TimeUnit.SECONDS);
		}
		for (int i = 0; i < 4; i++) {
			FCPexecutors.scheduleWithFixedDelay(new ProcessUploadUnfetchable(), 40 + i, 1, TimeUnit.SECONDS);
		}
		otherExecutors.schedule(new ProcessParse(), 2000, TimeUnit.MILLISECONDS);
		otherExecutors.scheduleWithFixedDelay(new Runnable() {
			public void run() {
				unfetchables.save(toUploadUnfetchable, toFetchUnfetchable);
			}
		}, 100, 20, TimeUnit.MINUTES);
		FcpSession session;
		try {
			session = new FcpSession("DownloadAllOnceFor" + u);
		} catch (IllegalStateException | IOException e1) {
			logger.log(Level.SEVERE, "Exception", e1);
			return;
		}
		try {
			startAndBlockUntilUpdate(session, u);
		} finally {
			shutdown(session);
		}
	}

	private class ParseQueues implements Runnable {
		private long count = 0;
		private long sum = 0;

		@Override
		public void run() {
			final Page page = toParse.poll();
			if (page != null) {
				otherExecutors.execute(new Runnable() {
					@Override
					public void run() {
						Date start = new Date();
						doParse(page);
						count++;
						sum += new Date().getTime() - start.getTime();
					}
				});

				otherExecutors.execute(this);
			} else {
				otherExecutors.schedule(this, 10, TimeUnit.SECONDS);
			}
		}

		long getMean() {
			if (count == 0) {
				return 1;
			}
			return sum / count;
		}
	}

	/**
	 * Queue pages to process from the Queues indefinitely.
	 *
	 * By scheduling a new queuing of pages directly after the processing of
	 * pages any amount of threads will be filled up with processing pages.
	 */
	private class QueueQueues implements Runnable {

		private boolean startedFetch = false;
		private boolean startedUpload = false;

		private Random random = new Random();

		private long nextLong(long l) {
			if (l <= 0) {
				l = 1L;
			}
			return (random.nextLong() >>> 1) % l;
		}

		/**
		 * Balance the fetches against the uploads by deciding if we shall upload
		 * or not. The length of the fetch queues is compared with the length of
		 * the upload queues considering the relationship between the estimated
		 * time for a fetch or an upload.
		 *
		 * @param ql Upload Queue Length to balance.
		 * @return <code>true</code> if we shall upload something.
		 */
		private boolean shallUpload(int ql) {
			return nextLong(uploadTime.get() * (toFetch.size() + toFetchUnfetchable.size()) / (1 + ql)
					/ fetchTime.get()) == 0;
		}

		private class Mean {
			private Queue<Long> whereToRead;
			private long count;
			private long sum;

			Mean(Queue<Long> w) {
				whereToRead = w;
				count = 0L;
				sum = 0L;
			}

			private void consume() {
				boolean done = false;
				do {
					Long found = whereToRead.poll();
					if (found != null) {
						count += 1;
						sum += found.longValue();
					} else {
						done = true;
					}
				} while (!done);
			}

			long get() {
				consume();
				if (count == 0) {
					return 1;
				}
				return sum / count;
			}
		}

		private final Queue<Long> fetchTimes = new LinkedBlockingQueue<Long>();
		private final Mean fetchTime = new Mean(fetchTimes);
		private final Queue<Long> uploadTimes = new LinkedBlockingQueue<Long>();
		private final Mean uploadTime = new Mean(uploadTimes);

		private class MeasureTime {
			private Queue<Long> whereToPost;
			private Date start;

			MeasureTime(Queue<Long> w) {
				whereToPost = w;
				start = new Date();
			}

			void done() {
				whereToPost.offer(new Date().getTime() - start.getTime());
			}
		}

		/**
		 * Normally starts one (1) fetch operation per invocation.
		 *
		 * If so, sets startedFetch.
		 */
		private void queueFetch() {
			{
				final Page page = toFetch.poll();
				if (page != null) {
					FCPexecutors.execute(new Runnable() {
						@Override
						public void run() {
							MeasureTime t = new MeasureTime(fetchTimes);
							boolean result = doFetch(page);
							t.done();
							logger.finest("Fetched Fetch" + (result ? "" : " failed") + ".");
						}
					});
					startedFetch = true;
				}
			}

			if (!startedFetch) {
				final Page page = toFetchUnfetchable.pollNotDeferred();
				if (page != null) {
					FCPexecutors.execute(new Runnable() {
						@Override
						public void run() {
							String log = page.logAttempts.toString();
							MeasureTime t = new MeasureTime(fetchTimes);
							boolean result = doFetchUnfetchable(page);
							t.done();
							logger.finer(log + "Fetched RefetchUnfetchable" + (result ? "" : " failed") + ".");
						}
					});
					startedFetch = true;
				}

				final Page page2 = toUploadUnfetchable.pollNotDeferred();
				if (page2 != null) {
					FCPexecutors.execute(new Runnable() {
						@Override
						public void run() {
							String log = page2.logAttempts.toString();
							MeasureTime t = new MeasureTime(fetchTimes);
							boolean result = doRefetchToUpload(page2);
							t.done();
							logger.finer(log + "Fetched ToUpload" + (result ? "" : " failed") + ".");
						}
					});
					startedFetch = true;
				}
			}

			if (!startedFetch || timesBelowSpaceThreshold > 0 || random.nextInt(200) == 0) {
				// We are queuing at least once.
				int laps = 0;
				do {
					laps++;
					final Page page = toRefetch.pollNotDeferred();
					if (page != null) {
						FCPexecutors.execute(new Runnable() {
							@Override
							public void run() {
								String log = page.logAttempts.toString();
								MeasureTime t = new MeasureTime(fetchTimes);
								boolean result = doRefetch(page);
								t.done();
								logger.finer(log + "Fetched Refetch" + (result ? "" : " failed") + ".");
							}
						});
						startedFetch = true;
					}
					// If there is not enough space left we are queuing three for every fetch,
					// if there is less than half of the space left we are queuing eight, 
					// if there is less than three times we are queuing 15 for every fetch...
					// This way, when the space is tight we are increasing the amount of
					// refetches over fetches.  This will slow down the fetches while the 
					// refetched files are cleaned out.
				} while (laps < (1 + timesBelowSpaceThreshold) * (1 + timesBelowSpaceThreshold));
			}
		}

		/**
		 * Sometimes starts upload operations. If it should or not, depends on the
		 * balance between the queue lengths and the fetch and upload times, see
		 * {@link #shallUpload(int)}.
		 *
		 * If an upload operations is started, sets startedUpload.
		 */
		private void queueUpload() {
			if (morePagesDirectory != null) {
				if (shallUpload(toFetchUnfetchable.size())) {
					final Page page = toFetchUnfetchable.poll();
					if (page != null) {
						FCPexecutors.execute(new Runnable() {
							@Override
							public void run() {
								MeasureTime t = new MeasureTime(uploadTimes);
								doCopyAndUploadUnfetchable(page);
								t.done();
							}
						});
						startedUpload = true;
					}
				}
			}

			if (shallUpload(toUploadUnfetchable.size())) {
				final Page page = toUploadUnfetchable.poll();
				if (page != null) {
					FCPexecutors.execute(new Runnable() {
						@Override
						public void run() {
							MeasureTime t = new MeasureTime(uploadTimes);
							boolean result = doUploadUnfetchable(page);
							t.done();
							logger.finer("Uploaded Unfetchable" + (result ? "" : "failed") + ".");
						}
					});
					startedUpload = true;
				}
			}
		}

		@Override
		public void run() {
			startedFetch = false;
			startedUpload = false;

			queueFetch();
			queueUpload();

			if (startedFetch || startedUpload) {
				FCPexecutors.execute(this);
			} else {
				FCPexecutors.schedule(this, 10, TimeUnit.SECONDS);
			}
		}

		public Object getFetchMean() {
			return fetchTime.get();
		}

		public Object getUploadMean() {
			return uploadTime.get();
		}
	}

	/**
	 * Starts the ball rolling for the FCPexecutors and otherExecutors.
	 *
	 * @param numThreads Number of threads for FCPexecutors.
	 * @param u URI pointing to the root of the tree.
	 * @param morePagesDir A directory where more pages can be found (or null). If
	 * non-null, this directory is searched for pages to upload
	 * if the download fails.
	 */
	private void run2(int numThreads, FreenetURI u, File morePagesDir) {
		morePagesDirectory = morePagesDir;
		FCPexecutors = Executors.newScheduledThreadPool(numThreads);
		otherExecutors = Executors.newScheduledThreadPool(1);
		try {
			directory = new File("library-download-all-once-db");
			if (directory.exists()) {
				unfetchables.load(directory);
				cleanUp = new CleanupOldFiles();
				cleanUp.setHandle(FCPexecutors.scheduleWithFixedDelay(cleanUp, 5, 1, TimeUnit.MINUTES));
			} else {
				directory.mkdir();
			}

			final ParseQueues pq = new ParseQueues();
			otherExecutors.execute(pq);
			final QueueQueues qq = new QueueQueues();
			otherExecutors.scheduleWithFixedDelay(new Runnable() {
				public void run() {
					logStatistics();
					logger.log(Level.INFO, "Parse time: {0} Fetch time: {1} Upload time: {2}",
							new Object[] {
									pq.getMean(), qq.getFetchMean(), qq.getUploadMean()
							});
				}
			}, 1, 1, TimeUnit.MINUTES);
			FCPexecutors.schedule(qq, 2, TimeUnit.SECONDS);
			FCPexecutors.scheduleWithFixedDelay(new Runnable() {
				public void run() {
					unfetchables.save(toUploadUnfetchable, toFetchUnfetchable);
				}
			}, 100, 20, TimeUnit.MINUTES);
			FcpSession session;
			try {
				session = new FcpSession("DownloadOneEditionFor" + u);
			} catch (IllegalStateException | IOException e1) {
				logger.log(Level.SEVERE, "Exception", e1);
				return;
			}
			try {
				startAndBlockUntilUpdate(session, u);
			} finally {
				shutdown(session);
			}
		} finally {
			if (FCPexecutors != null) {
				FCPexecutors.shutdown();
			}
			if (otherExecutors != null) {
				otherExecutors.shutdown();
			}
		}
	}

	private void startAndBlockUntilUpdate(FcpSession session, FreenetURI uri) {
		connection = session.getConnection();
		if (connection == null) {
			throw new IllegalArgumentException("No connection.");
		}
		// If the uri is given with the edition, restart at edition 0 anyway
		String uristring = uri.toString().replaceAll("/[0-9]*/?$", "/0");
		final SubscribeUSK subscriber = new SubscribeUSK(uristring, "USK");
		subscriber.setActive(true);
		final int[] editions = new int[1];
		final FreenetURI[] newUris = new FreenetURI[1];
		editions[0] = 0;
		FcpAdapter listener = new FcpAdapter() {
			@Override
			public void receivedSubscribedUSKUpdate(FcpConnection fcpConnection,
					SubscribedUSKUpdate subscribedUSKUpdate) {
				assert fcpConnection == connection;
				FreenetURI newUri;
				try {
					newUri = new FreenetURI(subscribedUSKUpdate.getURI());
				} catch (MalformedURLException e) {
					throw new RuntimeException(e);
				}
				if (subscribedUSKUpdate.isNewKnownGood() && !newUri.equals(newUris[0])) {
					newUris[0] = newUri;
					editions[0] = subscribedUSKUpdate.getEdition();
					synchronized (subscriber) {
						subscriber.notify();
					}
				}
			}
		};
		connection.addFcpListener(listener);

		synchronized (subscriber) {
			try {
				connection.sendMessage(subscriber);
				subscriber.wait(); // Wait until found
				doHandleNew(new Page(newUris[0], null));
				edition = editions[0];
				subscriber.wait(); // Work until next one found
				logger.info("Next edition seen.");
			} catch (InterruptedException e) {
				throw new RuntimeException("Subscription interrupted.");
			} catch (IOException e) {
				throw new RuntimeException("Subscription can't write.");
			}
		}
	}

	private boolean waitTermination(long ms) {
		boolean t1 = false;
		boolean t2 = false;
		try {
			t1 = FCPexecutors.awaitTermination(ms, TimeUnit.MILLISECONDS);
			t2 = otherExecutors.awaitTermination(1 + ms / 10, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			throw new RuntimeException("Waiting for jobs.");
		}
		return t1 && t2;
	}

	public static void main(String[] argv) throws InterruptedException {
		Integer numThreads;
		try {
			numThreads = new Integer(argv[0]);
		} catch (NumberFormatException e) {
			logger.log(Level.SEVERE, "First parameter must be a number, was " + argv[0] + ".", e);
			System.exit(2);
			return;
		}

		FreenetURI u;
		try {
			u = new FreenetURI(argv[1]);
		} catch (MalformedURLException e) {
			logger.log(Level.SEVERE, "Exception", e);
			System.exit(2);
			return;
		}

		File morePagesDir = null;
		if (argv.length > 2) {
			morePagesDir = new File(argv[2]);
			if (!morePagesDir.exists()) {
				logger.severe("Directory " + morePagesDir + " does not exist.");
				System.exit(2);
				return;
			}
			if (!morePagesDir.isDirectory()) {
				logger.severe("File " + morePagesDir + " is not a directory.");
				System.exit(2);
				return;
			}
		}

		if (numThreads.intValue() == 0) {
			new DownloadOneEdition().run(u, morePagesDir);
		} else {
			logger.info("Running with " + numThreads.intValue() + " threads.");
			new DownloadOneEdition().run2(numThreads.intValue(), u, morePagesDir);
		}
	}
}
