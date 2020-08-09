/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package freenet.library.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import freenet.library.io.serial.MapSerialiser;
import freenet.library.util.exec.TaskAbortException;

class SkelMapMapStoringSerialiserForTest<K, V> implements MapSerialiser<K, V> {
	final private Map<Long, Object> store = Collections.synchronizedMap(new HashMap<Long, Object>());
	private NumberGenerator generator;

	public SkelMapMapStoringSerialiserForTest(NumberGenerator generator) {
		this.generator = generator;
	}

	@Override
	public void pull(Map<K, PullTask<V>> tasks, Object mapmeta) throws TaskAbortException {
		for (Map.Entry<K, PullTask<V>> en : tasks.entrySet()) {
			en.getValue().data = ((Map<K, V>) store.get(en.getValue().meta)).get(en.getKey());
		}
	}

	@Override
	public void push(Map<K, PushTask<V>> tasks, Object mapmeta) throws TaskAbortException {
		Map<K, V> map = new HashMap<K, V>();
		for (Map.Entry<K, PushTask<V>> en : tasks.entrySet()) {
			map.put(en.getKey(), en.getValue().data);
		}
		Long pos = generator.next();
		store.put(pos, map);
		for (Map.Entry<K, PushTask<V>> en : tasks.entrySet()) {
			en.getValue().meta = pos;
		}
	}

}