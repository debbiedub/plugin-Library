package freenet.library.index;

/**
 * Internal message within the uploader to keep track of when the files
 * are added in the queue.
 */
public class TermInfoMessageEntry extends TermEntry {

	public TermInfoMessageEntry(String s) {
		super(s, 0.0f);
	}

	@Override
	public EntryType entryType() {
		return EntryType.INFO_MESSAGE;
	}

	@Override
	public boolean equalsTarget(TermEntry entry) {
		return entry == this || entry instanceof TermInfoMessageEntry;
	}

}
