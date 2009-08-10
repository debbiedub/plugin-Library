/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package plugins.Library.index;

import plugins.Library.Library;
import plugins.Library.util.SkeletonBTreeMap;
import plugins.Library.util.SkeletonBTreeSet;

import plugins.Library.serial.Serialiser.*;
import plugins.Library.serial.Serialiser;
import plugins.Library.serial.Translator;
import plugins.Library.serial.Archiver;
import plugins.Library.serial.FileArchiver;
import plugins.Library.serial.DataFormatException;
import plugins.Library.serial.TaskAbortException;
import plugins.Library.client.FreenetArchiver;
import plugins.Library.io.YamlReaderWriter;

import java.util.Collection;
import java.util.Set;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.LinkedHashMap;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.TreeMap;
import java.util.Date;

import freenet.keys.FreenetURI;

/**
** Serialiser for ProtoIndex
**
** DOCUMENT
**
** @author infinity0
*/
public class ProtoIndexSerialiser
implements Archiver<ProtoIndex>,
           Serialiser.Composite<Archiver<Map<String, Object>>>, // PRIORITY make this a LiveArchiver
           Serialiser.Translate<ProtoIndex, Map<String, Object>>/*,
           Serialiser.Trackable<Index>*/ {

	final protected static Translator<ProtoIndex, Map<String, Object>>
	trans = new IndexTranslator();

	final protected Archiver<Map<String, Object>>
	subsrl;

	public ProtoIndexSerialiser(Archiver<Map<String, Object>> s) {
		subsrl = s;
	}

	public ProtoIndexSerialiser(boolean test) {
		subsrl = new FileArchiver<Map<String, Object>>(ProtoIndexComponentSerialiser.yamlrw, true, ".yml");
	}

	public ProtoIndexSerialiser() {
		subsrl = Library.makeArchiver(ProtoIndexComponentSerialiser.yamlrw, "text/yaml", 0x10000);
	}

	@Override public Archiver<Map<String, Object>> getChildSerialiser() {
		return subsrl;
	}

	@Override public Translator<ProtoIndex, Map<String, Object>> getTranslator() {
		return trans;
	}

	@Override public void pull(PullTask<ProtoIndex> task) throws TaskAbortException {
		PullTask<Map<String, Object>> serialisable = new PullTask<Map<String, Object>>(task.meta);
		subsrl.pull(serialisable);
		serialisable.data.put("reqID", (task.meta = serialisable.meta) instanceof FreenetURI? task.meta: null); // so we can test on local files
		task.data = trans.rev(serialisable.data);
	}

	@Override public void push(PushTask<ProtoIndex> task) throws TaskAbortException {
		PushTask<Map<String, Object>> serialisable = new PushTask<Map<String, Object>>(trans.app(task.data));
		serialisable.meta = serialisable.data.remove("insID");
		subsrl.push(serialisable);
		task.meta = serialisable.meta;
	}

	public static class IndexTranslator
	implements Translator<ProtoIndex, Map<String, Object>> {

		/**
		** Term-table translator
		*/
		Translator<SkeletonBTreeMap<String, SkeletonBTreeSet<TermEntry>>, Map<String, Object>> ttrans = new
		SkeletonBTreeMap.TreeTranslator<String, SkeletonBTreeSet<TermEntry>>(null, new
		ProtoIndexComponentSerialiser.TreeMapTranslator<String, SkeletonBTreeSet<TermEntry>>(null));

		/**
		** URI-table translator
		*/
		Translator<SkeletonBTreeMap<URIKey, SkeletonBTreeMap<FreenetURI, URIEntry>>, Map<String, Object>> utrans = new
		SkeletonBTreeMap.TreeTranslator<URIKey, SkeletonBTreeMap<FreenetURI, URIEntry>>(null, new
		ProtoIndexComponentSerialiser.TreeMapTranslator<URIKey, SkeletonBTreeMap<FreenetURI, URIEntry>>(null));


		@Override public Map<String, Object> app(ProtoIndex idx) {
			if (!idx.ttab.isBare() || !idx.utab.isBare()) {
				throw new IllegalArgumentException("Data structure is not bare. Try calling deflate() first.");
			}
			Map<String, Object> map = new LinkedHashMap<String, Object>();
			map.put("serialVersionUID", idx.serialVersionUID);
			map.put("serialFormatUID", idx.serialFormatUID);
			map.put("insID", idx.insID);
			map.put("name", idx.name);
			map.put("modified", idx.modified);
			map.put("extra", idx.extra);
			map.put("utab", utrans.app(idx.utab));
			map.put("ttab", ttrans.app(idx.ttab));
			return map;
		}

		@Override public ProtoIndex rev(Map<String, Object> map) {
			long magic = (Long)map.get("serialVersionUID");

			if (magic == ProtoIndex.serialVersionUID) {
				try {
					ProtoIndexComponentSerialiser cmpsrl = ProtoIndexComponentSerialiser.get((Integer)map.get("serialFormatUID"));
					FreenetURI reqID = (FreenetURI)map.get("reqID");
					String name = (String)map.get("name");
					Date modified = (Date)map.get("modified");
					Map<String, Object> extra = (Map<String, Object>)map.get("extra");
					SkeletonBTreeMap<URIKey, SkeletonBTreeMap<FreenetURI, URIEntry>> utab = utrans.rev((Map<String, Object>)map.get("utab"));
					SkeletonBTreeMap<String, SkeletonBTreeSet<TermEntry>> ttab = ttrans.rev((Map<String, Object>)map.get("ttab"));

					return cmpsrl.setSerialiserFor(new ProtoIndex(reqID, name, modified, extra, utab, ttab));

				} catch (ClassCastException e) {
					// TODO maybe find a way to pass the actual bad data to the exception
					throw new DataFormatException("Badly formatted data", e, null);

				} catch (UnsupportedOperationException e) {
					throw new DataFormatException("Unrecognised format ID", e, map.get("serialFormatUID"), map, "serialFormatUID");

				}

			} else {
				throw new DataFormatException("Unrecognised serial ID", null, magic, map, "serialVersionUID");
			}
		}

	}

}
