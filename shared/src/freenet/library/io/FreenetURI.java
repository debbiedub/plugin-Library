/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package freenet.library.io;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URLEncoder;

import freenet.copied.Base64;

/**
 * This is a simpler implementation of the FreenetURI than {link freenet.keys.FreenetURI}.
 * 
 * It has part of the interface in the same way but it is simpler and local to the Library.
 */
public class FreenetURI {
	private String contents;

	public FreenetURI(String uri) throws MalformedURLException {
		contents = uri;
		if (contents.matches("^[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]-" +
							 "[0-9a-f][0-9a-f][0-9a-f][0-9a-f]-" + 
							 "[0-9a-f][0-9a-f][0-9a-f][0-9a-f]-" +
							 "[0-9a-f][0-9a-f][0-9a-f][0-9a-f]-" +
							 "[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]$")) {
			return;
		}
		if (!contents.startsWith("CHK@") &&
				!contents.startsWith("SSK@") &&
				!contents.startsWith("KSK@") &&
				!contents.startsWith("USK@")
				) {
			throw new MalformedURLException("Unhandled keytype: " + uri);
		}
		if (!contents.matches("^.*@(.*,.*,A.*|)$")) {
			throw new MalformedURLException("Cannot find cryptoKey and routingKey structure: " + uri);
		}
	}

	static final byte CHK = 1;
	static final byte SSK = 2;
	static final byte KSK = 3;
	static final byte USK = 4;
	static final short ClientCHK_EXTRA_LENGTH = 5;
	static final short ClientSSK_EXTRA_LENGTH = 5;

	/**
	 * This method can read the traditional BinaryKey-coded data, from Spider
	 * while mostly reading the simpler UTF-string key encoded with length 0.
	 *
	 * @param dis
	 * @return a FreenetURI with the read key.
	 * @throws IOException
	 */
	public static FreenetURI readFullBinaryKeyWithLength(
			DataInputStream dis1) throws IOException {
		int len = dis1.readShort();
		if (len != 0) {
			/**
			 * This is to be able to read the data created by Spider.
			 */
			byte[] buf = new byte[len];
			dis1.readFully(buf);
			ByteArrayInputStream bais = new ByteArrayInputStream(buf);
			DataInputStream dis = new DataInputStream(bais);
			byte type = dis.readByte();
			String keyType;
			if(type == CHK)
				keyType = "CHK";
    		else if(type == SSK)
    			keyType = "SSK";
    		else if(type == KSK)
    			keyType = "KSK";
    		else
    			throw new IOException("Unrecognized FreenetURI type " + type);
			byte[] routingKey = null;
			byte[] cryptoKey = null;
			byte[] extra = null;
			if((type == CHK) || (type == SSK)) {
				// routingKey is a hash, so is exactly 32 bytes
				routingKey = new byte[32];
				dis.readFully(routingKey);
				// cryptoKey is a 256 bit AES key, so likewise
				cryptoKey = new byte[32];
				dis.readFully(cryptoKey);
				// Number of bytes of extra depends on key type
				int extraLen;
				extraLen = (type == CHK ? ClientCHK_EXTRA_LENGTH : ClientSSK_EXTRA_LENGTH);
				extra = new byte[extraLen];
				dis.readFully(extra);
			}

			String docName = null;
			if(type != CHK)
				docName = dis.readUTF();
			int count = dis.readInt();
			String[] metaStrings = new String[count];
			for(int i = 0; i < metaStrings.length; i++)
				metaStrings[i] = dis.readUTF();

			StringBuilder b = new StringBuilder();

			b.append(keyType).append('@');
			if(!"KSK".equals(keyType)) {
				if(routingKey != null)
				    b.append(Base64.encode(routingKey));
				if(cryptoKey != null)
				    b.append(',').append(Base64.encode(cryptoKey));
				if(extra != null)
				    b.append(',').append(Base64.encode(extra));
				if(docName != null)
				    b.append('/');
			}

			if(docName != null)
				b.append(URLEncoder.encode(docName, "UTF-8"));

			for(int i = 0; i < metaStrings.length; i++) {
				b.append('/').append(URLEncoder.encode(metaStrings[i], "UTF-8"));
			}
			
			return new FreenetURI(b.toString());
		}

		return new FreenetURI(dis1.readUTF());
	}

	public FreenetURI intern() {
		return this;
	}

	/**
	 * This is not the real thing. Coded with the length 0, we use a UTF string
	 * with the key instead.
	 *
	 * @param dos
	 * @throws IOException
	 */
	public void writeFullBinaryKeyWithLength(DataOutputStream dos) throws IOException {
		dos.writeShort(0);
		dos.writeUTF(contents);
	}

	public boolean isUSK()
	throws MalformedURLException {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet.");
		// return false;
	}

	public FreenetURI sskForUSK() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet.");
		// return null;
	}

	public boolean isSSKForUSK() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet.");
		// return false;
	}

	public FreenetURI uskForSSK() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet.");
		// return null;
	}

	public long getEdition() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet.");
		// return 0;
	}

	public FreenetURI setMetaString(Object object) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet.");
		// return this;
	}

	public FreenetURI setSuggestedEdition(int i) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet.");
		// return this;
	}

	public String[] getAllMetaStrings() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet.");
		// return null;
	}

	public Object lastMetaString() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet.");
		// return null;
	}

	public Object getDocName() {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet.");
		// return null;
	}

	public FreenetURI pushMetaString(String string) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not implemented yet.");
		// return null;
	}

	@Override
	public int hashCode() {
		return this.getClass().hashCode() ^ contents.hashCode();
	}
	
	@Override
	public String toString() {
		return contents;
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof FreenetURI)) {
			return false;
		}
		return contents.equals(((FreenetURI) o).toString());
	}
}
