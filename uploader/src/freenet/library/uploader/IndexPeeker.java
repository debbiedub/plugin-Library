package freenet.library.uploader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import freenet.library.index.TermEntry;
import freenet.library.io.FreenetURI;
import freenet.library.io.YamlReaderWriter;
import freenet.library.util.SkeletonBTreeMap;
import freenet.library.util.SkeletonBTreeSet;

/**
 * Looks in the existing index and establishes what part of the tree a term exists in.
 *
 * When created the IndexPeeker is not fixed to any part of the tree. The first call to
 * {@link #includes(String)} will succeed and fixes the instance for that part.
 * Subsequent calls to {@link #includes(String)} succeeds if the term can be included
 * when processing the same part.
 *
 * Exception: As long as the terms are on the top level, the part of the tree is not fixed.
 */
class IndexPeeker {
	private File directory;
	private Set<String> topElements;
	private ChoosenSection activeSection = null;
	private ChoosenSection partiallyFixedSection = null;
	private LinkedHashMap<String, Object> tab;

	private static final SkeletonBTreeMap<String, SkeletonBTreeSet<TermEntry>> newtrees =
			new SkeletonBTreeMap<String, SkeletonBTreeSet<TermEntry>>(12);

	IndexPeeker(File dir) {
		LinkedHashMap<String, Object> ttab;
		directory = dir;
		String lastCHK = DirectoryUploader.readStringFrom(new File(directory, UploaderPaths.LAST_URL_FILENAME));
		String rootFilename = directory + "/" + UploaderPaths.LIBRARY_CACHE + "/" + lastCHK;
		try {
			LinkedHashMap<String, Object> top = (LinkedHashMap<String, Object>) new YamlReaderWriter().readObject(new FileInputStream(new File(rootFilename)));
			ttab = (LinkedHashMap<String, Object>) top.get("ttab");
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
			return;
		}

		LinkedHashMap<String, Object> topTtab = (LinkedHashMap<String, Object>) ttab.get("entries");
		topElements = new HashSet<String>(topTtab.keySet());
		tab = ttab;
	}

	private static int compare(String a, String b) {
		return SkeletonBTreeMap.compare(a, b, newtrees.comparator());
	}
	
	class ChoosenSection {
		String before;
		String after;
		
		/**
		 * A ChosenSection can only be created once per IndexPeeker.
		 * @param subj
		 */
		ChoosenSection(String previous, String next) {
			before = previous;
			after = next;
		}

		boolean includes(String subj) {
			if ((before == null || compare(before, subj) < 0) &&
					(after == null || compare(subj, after) < 0)) {
				return true;
			}
			return false;
		}
	}
	
	/**
	 * If the subj is to be included.
	 * 
	 * If subj is on top, include it.
	 * Let the first subj decide what part of the tree we match.
	 * Include subsequent terms if they are in the same part of the tree.
	 * 
	 * @param subj The term to include.
	 * @return true if the term is included.
	 */
	boolean includes(String subj) {
		if (topElements.contains(subj)) {
			if (partiallyFixedSection != null) {
				if (!partiallyFixedSection.includes(subj)) {
					return false;
				}
			}
			return true;
		}
		if (activeSection != null) {
			return activeSection.includes(subj);
		}
		assert activeSection == null;
		String seenPrevious = null;
		String seenNext = null;
		String previous = null;
		String next = null;
		while (tab != null) {
			previous = null;
			next = null;
			int subnodesIndex = 0;
			Set<String> entries = ((LinkedHashMap<String, Object>) tab.get("entries")).keySet();
			for (String iter : entries) {
				next = iter;
				if (compare(subj, next) < 0) {
					break;
				}
				previous = iter;
				next = null;
				subnodesIndex ++;
			}
			if (previous != null) {
				seenPrevious = previous;
			}
			if (next != null) {
				seenNext = next;
			}
			LinkedHashMap<Object, Object> subnodes = (LinkedHashMap<Object, Object>) tab.get("subnodes");
			List<Object> subnodesList = new ArrayList<Object>(subnodes.keySet());
			Object uriAsObject = subnodesList.get(subnodesIndex);
			FreenetURI uri;
			if (uriAsObject instanceof FreenetURI) {
				uri = (FreenetURI) uriAsObject;
			} else {
				try {
					uri = new FreenetURI((String) uriAsObject);
				} catch (MalformedURLException e) {
					throw new RuntimeException("Invalid uri "+ uriAsObject + " in list.", e);
				}
			}

			String filename = directory + "/" + UploaderPaths.LIBRARY_CACHE + "/" + uri;
			LinkedHashMap<String, Object> top;
			try {
				top = (LinkedHashMap<String, Object>) new YamlReaderWriter().readObject(new FileInputStream(new File(filename)));
			} catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
				return false;
			}
			if (top.containsKey("subnodes")) {
				topElements.addAll(((LinkedHashMap<String, Object>) top.get("entries")).keySet());
				tab = top;
				if (topElements.contains(subj)) {
					partiallyFixedSection = new ChoosenSection(previous, next);
					return true;
				}
			} else {
				tab = null;
				System.out.println("Grouping around " + subj);
				partiallyFixedSection = null;
				break;
			}
		}
		activeSection = new ChoosenSection(seenPrevious, seenNext);
		return true;
	}
}
