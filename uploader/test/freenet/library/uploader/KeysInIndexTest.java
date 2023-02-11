/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package freenet.library.uploader;

import static org.junit.Assert.assertEquals;

import java.net.MalformedURLException;

import org.junit.Test;

import freenet.library.io.FreenetURI;

public class KeysInIndexTest {

	KeysInIndex index = new KeysInIndex();

	@Test
	public void empty() {
		assertEquals(index.list.size(), 0);
		assertEquals(index.editions.size(), 0);
	}

	@Test
	public void overwrite() throws MalformedURLException {
		FreenetURI uri = new FreenetURI("USK@kryptoKey,routingKey,ASET/name/4/extra");
		index.add(uri);
		assertEquals(index.list.size(), 1);
		FreenetURI uri2 = uri.setSuggestedEdition(5L);
		index.add(uri2);
		assertEquals(index.list.size(), 1);
	}
}
