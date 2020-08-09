/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package freenet.library.util;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.UUID;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import freenet.library.util.SkeletonBTreeMap;
import freenet.library.util.exec.TaskAbortException;

import static org.junit.Assert.*;

public class SkeletonBTreeMapTest {

	SkeletonBTreeMap<String, Integer> skelmap;
	String lastAddedKey;
	Set<String> allKeys;

	private static String rndStr() {
		return UUID.randomUUID().toString();
	}

	private static String rndKey() {
		return rndStr().substring(0, 8);
	}

	SkelMapMapStoringSerialiserForTest<String, Integer> lastMapSerialiser;

	@Before
	public void setUp() throws TaskAbortException {
		skelmap = new SkeletonBTreeMap<String, Integer>(2);
		ReferenceGenerator generator = new ReferenceGenerator();
		lastMapSerialiser = new SkelMapMapStoringSerialiserForTest<String, Integer>(generator);
		skelmap.setSerialiser(
				new SkelMapNodeStoringSerialiserForTest<String, Integer>(skelmap, lastMapSerialiser, generator),
				lastMapSerialiser);
		assertTrue(skelmap.isBare());

		allKeys = new TreeSet<String>();
	}

	private void add(int laps, int count) throws TaskAbortException {
		int calculatedSize = skelmap.size();
		for (int l = 0; l < laps; ++l) {
			SortedMap<String, Integer> map = new TreeMap<String, Integer>();
			for (int i = 0; i < count; ++i) {
				String key = rndKey();
				map.put(key, i);
				allKeys.add(key);
				lastAddedKey = key;
			}
			skelmap.update(map, new TreeSet<String>());
			calculatedSize += count;
			assertTrue(skelmap.isBare());
			assertEquals(calculatedSize, skelmap.size());
		}
	}

	private void checkAllKeys() throws TaskAbortException {
		for (String k : allKeys) {
			skelmap.inflate(k);
			assertNotNull(skelmap.getDeflateRest(k));
		}
		skelmap.deflate();
		skelmap.isBare();
	}

	@Test
	public void testSetup() {
		assertTrue(true);
	}

	@Test
	public void test1() throws TaskAbortException {
		add(1, 1);

		checkAllKeys();
	}

	@Test
	public void test3() throws TaskAbortException {
		add(1, 3);

		checkAllKeys();
	}

	@Test
	public void test4() throws TaskAbortException {
		add(1, 4);

		checkAllKeys();
	}

	@Test
	public void test10() throws TaskAbortException {
		add(1, 10);

		checkAllKeys();
	}

	@Test
	public void test100() throws TaskAbortException {
		add(1, 100);

		checkAllKeys();
	}

	@Ignore("Takes too long to run.")
	@Test
	public void BIGtest1000() throws TaskAbortException {
		add(1, 1000);

		checkAllKeys();
	}

	@Ignore("Takes too long to run.")
	@Test
	public void BIGtest10000() throws TaskAbortException {
		add(1, 10000);

		checkAllKeys();
	}

	@Test
	public void test1x3() throws TaskAbortException {
		add(3, 1);

		checkAllKeys();
	}

	@Test
	public void test1x4() throws TaskAbortException {
		add(4, 1);

		checkAllKeys();
	}

	@Test
	public void test1x5() throws TaskAbortException {
		add(5, 1);

		checkAllKeys();
	}

	@Test
	public void test6x5() throws TaskAbortException {
		add(5, 6);

		checkAllKeys();
	}

	@Ignore("Takes too long to run.")
	@Test
	public void test10x5() throws TaskAbortException {
		add(5, 10);

		checkAllKeys();
	}

	@Ignore("Takes too long to run.")
	@Test
	public void BIGtest10x50() throws TaskAbortException {
		add(50, 10);

		checkAllKeys();
	}
}
