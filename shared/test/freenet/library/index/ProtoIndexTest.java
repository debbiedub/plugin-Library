/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package freenet.library.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;

import freenet.library.ArchiverFactory;
import freenet.library.FactoryRegister;
import freenet.library.Priority;
import freenet.library.io.FreenetURI;
import freenet.library.io.ObjectStreamReader;
import freenet.library.io.ObjectStreamWriter;
import freenet.library.io.serial.LiveArchiver;
import freenet.library.io.serial.Serialiser.PushTask;
import freenet.library.util.SkeletonBTreeSet;
import freenet.library.util.concurrent.ExceptionConvertor;
import freenet.library.util.exec.SimpleProgress;
import freenet.library.util.exec.TaskAbortException;
import freenet.library.util.func.Closure;

public class ProtoIndexTest {

	private MockLiveArchiver mockLiveArchiver;
	private MockArchiverFactory mockArchiverFactory;
	private ProtoIndexSerialiser mockProtoIndexSerialiser;
	private ProtoIndex mockProtoIndex;
	private ProtoIndexComponentSerialiser leafsrl;

	int archiverResultNumber = 1;
	Map<Object, String> store = new HashMap<Object, String>();

    /**
     * For pull, the meta is a String containing the contents of the stream.
     */
	class MockLiveArchiver implements LiveArchiver<Map<String, Object>, SimpleProgress> {
		ObjectStreamReader<Map<String, Object>> reader;
		ObjectStreamWriter<Map<String, Object>> writer;
		MockLiveArchiver(ObjectStreamReader<Map<String, Object>> r,
				ObjectStreamWriter<Map<String, Object>> w) {
			reader = r;
			writer = w;
		}
		
		byte[] bytesToParse;
		String createdOutput;
		
		@Override
		public void pull(
				freenet.library.io.serial.Serialiser.PullTask<Map<String, Object>> task)
				throws TaskAbortException {
			bytesToParse = store.get(task.meta).getBytes();
			assertNotNull(bytesToParse);
			InputStream is = new ByteArrayInputStream(bytesToParse);
			try {
				task.data = reader.readObject(is);
			} catch (IOException e) {
				throw new TaskAbortException("byte array unparseable", e);
			}
		}

		@Override
		public void push(
				freenet.library.io.serial.Serialiser.PushTask<Map<String, Object>> task)
				throws TaskAbortException {
			ByteArrayOutputStream os = new ByteArrayOutputStream();
			try {
				writer.writeObject(task.data, os);
			} catch (IOException e) {
				throw new TaskAbortException("Could not write", e);
			}
			createdOutput = os.toString();
		}

		@Override
		public void pullLive(
				freenet.library.io.serial.Serialiser.PullTask<Map<String, Object>> task,
				SimpleProgress p) throws TaskAbortException {
			if (p != null) {
				p.addPartKnown(1, true);
			}
			pull(task);
			if (p != null) {
				p.addPartDone();
			}
		}

		@Override
		public void pushLive(
				freenet.library.io.serial.Serialiser.PushTask<Map<String, Object>> task,
				SimpleProgress p) throws TaskAbortException {
			if (p != null) {
				p.addPartKnown(1, true);
			}
			push(task);
			try {
				task.meta = new FreenetURI("CHK@" + (archiverResultNumber++) + ",7,A6");
				store.put(task.meta, createdOutput);
			} catch (MalformedURLException e) {
				throw new TaskAbortException("URL problem", e);
			}
			if (p != null) {
				p.addPartDone();
			}
		}

		@Override
		public void waitForAsyncInserts() throws TaskAbortException {
			// Do nothing
		}
	}

	class MockArchiverFactory implements ArchiverFactory {

		@Override
		public <T, S extends ObjectStreamWriter & ObjectStreamReader> LiveArchiver<T, SimpleProgress> newArchiver(
				S rw, String mime, int size, Priority priorityLevel) {
			assertNotNull(rw);
			assertEquals(ProtoIndexComponentSerialiser.yamlrw, rw);
			assertNotNull(mime);
			assertNotSame(0, size);
			assertEquals(Priority.Bulk, priorityLevel);
			return (LiveArchiver<T, SimpleProgress>) new MockLiveArchiver(rw, rw);
		}

		@Override
		public <T, S extends ObjectStreamWriter & ObjectStreamReader> LiveArchiver<T, SimpleProgress> newArchiver(
				S rw, String mime, int size,
				LiveArchiver<T, SimpleProgress> archiver) {
			fail("Not called by the tests.");
			return null;
		}
	}

	@Before
	public void setUp() throws Exception {
		mockArchiverFactory = new MockArchiverFactory();
		FactoryRegister.register(mockArchiverFactory);
		mockLiveArchiver = new MockLiveArchiver(
				ProtoIndexComponentSerialiser.yamlrw,
				ProtoIndexComponentSerialiser.yamlrw);

		mockProtoIndexSerialiser = new ProtoIndexSerialiser(mockLiveArchiver);
		
		FreenetURI reqID = null;
		mockProtoIndex = new ProtoIndex(reqID, "name", "owner", "email", 0);

		ProtoIndexSerialiser srl = ProtoIndexSerialiser.forIndex(new FreenetURI("CHK@"), Priority.Bulk);
		LiveArchiver<Map<String,Object>,SimpleProgress> archiver = 
				(LiveArchiver<Map<String,Object>,SimpleProgress>)(srl.getChildSerialiser());
		leafsrl = ProtoIndexComponentSerialiser.get(ProtoIndexComponentSerialiser.FMT_DEFAULT, archiver);
	}

	/**
	 * Testing to add elements, one at the time, and then check that some of them
	 * are there.
	 *
	 * @throws TaskAbortException
	 * @throws MalformedURLException
	 */
	@Test
	public void testPushContents() throws TaskAbortException, MalformedURLException {
		final int ENTRIES = 10000;
		for (int i = 0; i < ENTRIES; i++) {
			final SkeletonBTreeSet<TermEntry> value = new SkeletonBTreeSet<TermEntry>(100);
			value.add(new TermPageEntry("a", 1, new FreenetURI("CHK@1,2,A3"), "title", null));
			leafsrl.setSerialiserFor(value);
			value.deflate();
	
			mockProtoIndex.ttab.put("a" + i, value);
		}

		leafsrl.setSerialiserFor(mockProtoIndex);
		mockProtoIndex.ttab.deflate();

		mockLiveArchiver.waitForAsyncInserts();

		PushTask<ProtoIndex> task = new PushTask<ProtoIndex>(mockProtoIndex);
		mockProtoIndexSerialiser.push(task);

		assertTrue(mockLiveArchiver.createdOutput.contains("serialVersionUID: " + ProtoIndex.serialVersionUID));
		final String emptyBTree = "\n  node_min: 1024\n  size: 0\n  entries: {}\n";
		assertTrue(mockLiveArchiver.createdOutput.contains("\nutab:" + emptyBTree));
		final String countBTreeProlog = "\n  node_min: 1024\n  size: " + ENTRIES + "\n  entries:\n";
		assertTrue(mockLiveArchiver.createdOutput.contains("\nttab:" + countBTreeProlog));

		for (int i = 0; i < ENTRIES; i += 267) {
			mockProtoIndex.ttab.inflate("a" + i);
			assertTrue(mockProtoIndex.ttab.containsKey("a" + i));
			mockProtoIndex.ttab.deflate();
			mockLiveArchiver.waitForAsyncInserts();
		}
	}

	/**
	 * Test the set that is the contents of a term.
	 * @throws TaskAbortException
	 * @throws MalformedURLException
	 */
	@Test
	public void testPushOne() throws TaskAbortException, MalformedURLException {
		final SkeletonBTreeSet<TermEntry> value = new SkeletonBTreeSet<TermEntry>(100);
		final int ENTRIES = 3000;
		for (int i = 0; i < ENTRIES; i++) {
			value.add(new TermPageEntry("adam", 1, new FreenetURI("CHK@" + i + ",2,A3"), "title", null));
		}
		leafsrl.setSerialiserFor(value);
		value.deflate();

		mockProtoIndex.ttab.put("adam", value);

		leafsrl.setSerialiserFor(mockProtoIndex);
		mockProtoIndex.ttab.deflate();

		mockLiveArchiver.waitForAsyncInserts();

		assertTrue(mockProtoIndex.ttab.containsKey("adam"));
		assertFalse(mockProtoIndex.ttab.containsKey("eve"));

		mockProtoIndex.ttab.inflate();
		final SkeletonBTreeSet<TermEntry> set = mockProtoIndex.ttab.get("adam");
		set.inflate();
		for (int i = 0; i < ENTRIES; i += 329) {
			TermPageEntry entry = new TermPageEntry("adam", 1, new FreenetURI("CHK@" + i + ",2,A3"), "title", null);
			assertTrue(set.contains(entry));
		}
		set.deflate();
		mockLiveArchiver.waitForAsyncInserts();
		mockProtoIndex.ttab.deflate();

		mockLiveArchiver.waitForAsyncInserts();
	}
	
	/**
	 * Test adding using value handler.
	 * @throws TaskAbortException
	 * @throws MalformedURLException
	 */
	@Test
	public void testValueHandler() throws TaskAbortException, MalformedURLException {
		leafsrl.setSerialiserFor(mockProtoIndex);

		final int ENTRIES_PER_VALUE_HANDLER = 800;
		SortedSet<String> putkey = new TreeSet<String>();
		putkey.add("adam");
		SortedSet<String> remkey = null;
		Closure<Entry<String, SkeletonBTreeSet<TermEntry>>, Exception> value_handler =
				new Closure<Entry<String, SkeletonBTreeSet<TermEntry>>, Exception>() {
					int index = 0;
					@Override
					public void invoke(Entry<String, SkeletonBTreeSet<TermEntry>> param) throws Exception {
						if (param.getValue() == null) {
							final SkeletonBTreeSet<TermEntry> value = new SkeletonBTreeSet<TermEntry>(100);
							value.add(new TermPageEntry("adam", 1, new FreenetURI("CHK@" + index++ + ",2,A3"), "title", null));
							leafsrl.setSerialiserFor(value);
							value.deflate();
							param.setValue(value);
						} else {
							param.getValue().inflate();
							for (int i = 0; i < ENTRIES_PER_VALUE_HANDLER; i++) {
								param.getValue().add(new TermPageEntry("adam", 1, new FreenetURI("CHK@" + index++ + ",2,A3"), "title", null));
							}
							param.getValue().deflate();
						}
					}
		};
		ExceptionConvertor<Exception> conv = null;

		mockProtoIndex.ttab.update(putkey, remkey, value_handler, conv);
		final int ENTRIES = 3000;
		for (int i = 0; i < ENTRIES; i += ENTRIES_PER_VALUE_HANDLER) {
			mockProtoIndex.ttab.update(putkey, remkey, value_handler, conv);
		}

		mockProtoIndex.ttab.deflate();

		mockLiveArchiver.waitForAsyncInserts();

		assertTrue(mockProtoIndex.ttab.containsKey("adam"));
		assertFalse(mockProtoIndex.ttab.containsKey("eve"));

		mockProtoIndex.ttab.inflate();
		final SkeletonBTreeSet<TermEntry> set = mockProtoIndex.ttab.get("adam");
		for (int i = 0; i < ENTRIES; i += 329) {
			TermPageEntry entry = new TermPageEntry("adam", 1, new FreenetURI("CHK@" + i + ",2,A3"), "title", null);
			set.inflate();
			assertTrue(set.contains(entry));
			set.deflate();
			mockLiveArchiver.waitForAsyncInserts();
		}
		mockProtoIndex.ttab.deflate();

		mockLiveArchiver.waitForAsyncInserts();
	}
	
	
	/**
	 * Test removing a key using update.
	 * @throws TaskAbortException
	 * @throws MalformedURLException
	 */
	// @Test
	// I will wait with this one. The removing a member of the set is more interesting and simpler.
	public void testRemoveKey() throws TaskAbortException, MalformedURLException {
		final int ENTRIES = 10000;
		for (int i = 0; i < ENTRIES; i++) {
			final SkeletonBTreeSet<TermEntry> value = new SkeletonBTreeSet<TermEntry>(100);
			value.add(new TermPageEntry("a", 1, new FreenetURI("CHK@1,2,A3"), "title", null));
			leafsrl.setSerialiserFor(value);
			value.deflate();
	
			mockProtoIndex.ttab.put("a" + i, value);
		}

		leafsrl.setSerialiserFor(mockProtoIndex);
		mockProtoIndex.ttab.deflate();
		mockLiveArchiver.waitForAsyncInserts();

		SortedSet<String> putkey = null;
		SortedSet<String> remkey = new TreeSet<String>();
		remkey.add("a1");
		remkey.add("a3473");
		remkey.add("a7284");
		Closure<Entry<String, SkeletonBTreeSet<TermEntry>>, Exception> value_handler =
				new Closure<Entry<String, SkeletonBTreeSet<TermEntry>>, Exception>() {
					@Override
					public void invoke(Entry<String, SkeletonBTreeSet<TermEntry>> param) throws Exception {
						fail("Value handler called on remove key.");
					}
		};
		ExceptionConvertor<Exception> conv = null;

		mockProtoIndex.ttab.update(putkey, remkey, value_handler, conv);
		mockProtoIndex.ttab.deflate();
		mockLiveArchiver.waitForAsyncInserts();

		assertKeyDoesNotExist("a1");
		assertKeyExists("a2");
		assertKeyExists("a3472");
		assertKeyDoesNotExist("a3473");
		assertKeyExists("a3474");
		assertKeyExists("a7283");
		assertKeyDoesNotExist("a7284");
		assertKeyExists("a7285");
	}

	private void assertKeyDoesNotExist(String key) throws TaskAbortException {
		mockProtoIndex.ttab.inflate(key);
		assertFalse(mockProtoIndex.ttab.containsKey(key));
		mockProtoIndex.ttab.deflate();
		mockLiveArchiver.waitForAsyncInserts();
	}

	private void assertKeyExists(String key) throws TaskAbortException {
		mockProtoIndex.ttab.inflate(key);
		assertTrue(mockProtoIndex.ttab.containsKey(key));
		mockProtoIndex.ttab.deflate();
		mockLiveArchiver.waitForAsyncInserts();
	}

	/**
	 * Test removing members in set using the value handler.
	 * @throws TaskAbortException
	 * @throws MalformedURLException
	 */
	@Test
	public void testRemoveMembersInSet() throws TaskAbortException, MalformedURLException {
		final int ENTRIES = 10;
		final int MEMBERS = 2000;
		for (int i = 0; i < ENTRIES; i++) {
			final SkeletonBTreeSet<TermEntry> value = new SkeletonBTreeSet<TermEntry>(100);
			for (int j = 0; j < MEMBERS; j++) {
				value.add(createTermPageEntry(i, j));
			}
			leafsrl.setSerialiserFor(value);
			value.deflate();
	
			mockProtoIndex.ttab.put("a" + i, value);
		}
		leafsrl.setSerialiserFor(mockProtoIndex);
		mockProtoIndex.ttab.deflate();
		mockLiveArchiver.waitForAsyncInserts();

		SortedSet<String> putkey = new TreeSet<String>();
		putkey.add("a1");
		SortedSet<String> remkey = null;
		Closure<Entry<String, SkeletonBTreeSet<TermEntry>>, Exception> value_handler =
				new Closure<Entry<String, SkeletonBTreeSet<TermEntry>>, Exception>() {
					@Override
					public void invoke(Entry<String, SkeletonBTreeSet<TermEntry>> param) throws Exception {
						assertNotNull(param.getValue());
						int i = Integer.parseInt(param.getKey().substring(1));
						param.getValue().inflate();
						for (int j = 0; j < 10; j++) {
							param.getValue().remove(createTermPageEntry(i, j));
						}
						param.getValue().deflate();
					}
		};
		ExceptionConvertor<Exception> conv = null;
		mockProtoIndex.ttab.update(putkey, remkey, value_handler, conv);

		mockProtoIndex.ttab.inflate();
		final SkeletonBTreeSet<TermEntry> set = mockProtoIndex.ttab.get("a1");
		set.inflate();
		for (int j = 0; j < MEMBERS; j += 1) {
			if (j < 10) {
				assertFalse(set.contains(createTermPageEntry(1, j)));
			} else {
				assertTrue(set.contains(createTermPageEntry(1, j)));
			}
		}
		set.deflate();
		mockProtoIndex.ttab.deflate();
		mockLiveArchiver.waitForAsyncInserts();
	}

	private TermPageEntry createTermPageEntry(int i, int j) throws MalformedURLException {
		return new TermPageEntry("a" + i, 1, new FreenetURI("CHK@b" + j + "a,2,A3"), "title", null);
	}

	// remove last member in set
	/**
	 * Test removing the last member in the set. It is left with an empty set.
	 * @throws TaskAbortException
	 * @throws MalformedURLException
	 */
	@Test
	public void testRemoveLastMemberInSet() throws TaskAbortException, MalformedURLException {
		final SkeletonBTreeSet<TermEntry> value = new SkeletonBTreeSet<TermEntry>(100);
		TermPageEntry entry = new TermPageEntry("a", 1, new FreenetURI("CHK@1,2,A3"), "title", null);
		value.add(entry);
		leafsrl.setSerialiserFor(value);
		value.deflate();
		mockProtoIndex.ttab.put("a", value);

		leafsrl.setSerialiserFor(mockProtoIndex);
		mockProtoIndex.ttab.deflate();
		mockLiveArchiver.waitForAsyncInserts();

		PushTask<ProtoIndex> task = new PushTask<ProtoIndex>(mockProtoIndex);
		mockProtoIndexSerialiser.push(task);

		SortedSet<String> putkey = new TreeSet<String>();
		putkey.add("a");
		SortedSet<String> remkey = null;
		Closure<Entry<String, SkeletonBTreeSet<TermEntry>>, Exception> value_handler =
				new Closure<Entry<String, SkeletonBTreeSet<TermEntry>>, Exception>() {
					@Override
					public void invoke(Entry<String, SkeletonBTreeSet<TermEntry>> param) throws Exception {
						assertEquals("a", param.getKey());
						assertNotNull(param.getValue());
						param.getValue().inflate();
						param.getValue().remove(entry);
						param.getValue().deflate();
					}
		};
		ExceptionConvertor<Exception> conv = null;
		mockProtoIndex.ttab.update(putkey, remkey, value_handler, conv);

		final String countBTreeProlog = "\n  node_min: 1024\n  size: " + 1 + "\n  entries:\n";
		assertTrue(mockLiveArchiver.createdOutput.contains("\nttab:" + countBTreeProlog));

		mockProtoIndex.ttab.inflate("a");
		assertTrue(mockProtoIndex.ttab.containsKey("a"));
		final SkeletonBTreeSet<TermEntry> set = mockProtoIndex.ttab.get("a");
		set.inflate();
		assertTrue(set.isEmpty());
		set.deflate();
		mockProtoIndex.ttab.deflate();
		mockLiveArchiver.waitForAsyncInserts();
	}

}
