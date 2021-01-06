/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package freenet.library.uploader;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import freenet.copied.Base64;
import freenet.copied.SHA256;
import freenet.library.io.FreenetURI;
import freenet.library.io.ObjectStreamReader;
import freenet.library.io.ObjectStreamWriter;
import freenet.library.io.serial.LiveArchiver;
import freenet.library.util.exec.SimpleProgress;
import freenet.library.util.exec.TaskAbortException;

/**
 * An Archiver that only reads from disk.
 *
 * @param <T>
 * @param <S>
 */
public class DiskReader<T,  S extends ObjectStreamWriter & ObjectStreamReader> 
		implements LiveArchiver<T, freenet.library.util.exec.SimpleProgress> {
	private File cacheDir;
	private ObjectStreamReader<T> reader;

	public DiskReader(
			File directory,
			S rw,
			String mime, int s) {
		cacheDir = directory;
		reader = rw;
	}
	
	@Override
	public void pull(freenet.library.io.serial.Serialiser.PullTask<T> task)
			throws TaskAbortException {
		pullLive(task, null);
	}

	@Override
	public void push(freenet.library.io.serial.Serialiser.PushTask<T> task)
			throws TaskAbortException {
		pushLive(task, null);
	}

	/**
	 * Fetch everything from the disk cache. 
	 */
	@Override
	public void pullLive(freenet.library.io.serial.Serialiser.PullTask<T> task,
			SimpleProgress progress) throws TaskAbortException {
		if (cacheDir.exists()) {
			String cacheKey = null;
			if (task.meta instanceof FreenetURI) {
				cacheKey = task.meta.toString();
			} else if (task.meta instanceof String) {
				cacheKey = (String) task.meta;
			} else if (task.meta instanceof byte[]) {
				cacheKey = Base64.encode(SHA256.digest((byte[]) task.meta));
			}

			try {
				if(cacheDir != null && cacheDir.exists() && cacheDir.canRead()) {
					File cached = new File(cacheDir, cacheKey);
					if(cached.exists() && 
							cached.length() != 0 &&
							cached.canRead()) {
						InputStream is = new FileInputStream(cached);
						task.data = (T) reader.readObject(is);
						is.close();
					}
				}
					
				if (progress != null) {
					progress.addPartKnown(0, true);
				}
			} catch (IOException e) {
				System.out.println("IOException:");
				e.printStackTrace();
				throw new TaskAbortException("Failed to read content from local tempbucket", e, true);
			}
			return;
		}
		throw new UnsupportedOperationException(
				"Cannot find the key " +
				task.meta +
				" in the cache.");
	}

	@Override
	public void pushLive(freenet.library.io.serial.Serialiser.PushTask<T> task,
			SimpleProgress progress) throws TaskAbortException {
    	if (progress != null) {
    		progress.addPartKnown(1, true);
    	}

    	// This is a one-time read so we don't care about the result.
		task.data = null;
		
        if (progress != null) {
        	progress.addPartDone();
        }

	}

	@Override
	public void waitForAsyncInserts() throws TaskAbortException {
	}
}
