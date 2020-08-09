/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package freenet.library.util;

import static freenet.library.util.func.Tuples.X2;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;

import freenet.library.io.DataFormatException;
import freenet.library.io.serial.IterableSerialiser;
import freenet.library.io.serial.ScheduledSerialiser;
import freenet.library.io.serial.Translator;
import freenet.library.util.concurrent.Executors;
import freenet.library.util.concurrent.ObjectProcessor;
import freenet.library.util.exec.TaskAbortException;

class SkelMapNodeStoringSerialiserForTest<K, V> implements IterableSerialiser<SkeletonBTreeMap<K, V>.SkeletonNode>,
		ScheduledSerialiser<SkeletonBTreeMap<K, V>.SkeletonNode> {

	final private Map<Long, Object> store = Collections.synchronizedMap(new HashMap<Long, Object>());
	SkelMapMapStoringSerialiserForTest<K, V> mapSerialiser;

	Translator<SkeletonBTreeMap<K, V>, Map<String, Object>> ttrans = new SkeletonBTreeMap.TreeTranslator<K, V>(null,
			null);

	SkeletonBTreeMap<K, V>.NodeTranslator<K, Map<String, Object>> ntrans;

	private NumberGenerator generator;

	Translator<SkeletonTreeMap<K, V>, Map<String, Object>> tmtrans = new SkeletonTreeMap.TreeMapTranslator<K, V>() {

		@Override
		public Map<String, Object> app(SkeletonTreeMap<K, V> translatee) {
			return app(translatee, new TreeMap<String, Object>(), null);
		}

		@Override
		public SkeletonTreeMap<K, V> rev(Map<String, Object> intermediate) throws DataFormatException {
			return rev(intermediate, new SkeletonTreeMap<K, V>(), null);
		}
	};

	SkelMapNodeStoringSerialiserForTest(SkeletonBTreeMap<K, V> skelmap, SkelMapMapStoringSerialiserForTest<K, V> ms,
			NumberGenerator generator) {
		mapSerialiser = ms;
		ntrans = skelmap.makeNodeTranslator(null, tmtrans);
		this.generator = generator;
	}

	@Override
	public void pull(PullTask<SkeletonBTreeMap<K, V>.SkeletonNode> task) throws TaskAbortException {
		assert task != null;
		assert task.meta != null;
		assert task.meta instanceof SkeletonBTreeMap.GhostNode;
		SkeletonBTreeMap<K, V>.GhostNode gn = (SkeletonBTreeMap<K, V>.GhostNode) task.meta;
		assert gn.meta instanceof Long;
		assert store.containsKey(gn.meta);
		Map<String, Object> map = (Map<String, Object>) store.get(gn.meta);
		SkeletonBTreeMap<K, V>.SkeletonNode node;
		try {
			node = ntrans.rev(map);
		} catch (DataFormatException e) {
			throw new TaskAbortException("Unpacking SkeletonNode", e);
		}
		task.data = node;
	}

	@Override
	public void push(PushTask<SkeletonBTreeMap<K, V>.SkeletonNode> task) throws TaskAbortException {
		assert task.data.isBare();
		Map<String, Object> map = ntrans.app(task.data);

		Long pos = generator.next();
		store.put(pos, map);
		task.meta = task.data.makeGhost(pos);
	}

	@Override
	public void pull(Iterable<PullTask<SkeletonBTreeMap<K, V>.SkeletonNode>> tasks) throws TaskAbortException {
		throw new TaskAbortException("NIY", new Throwable());
	}

	@Override
	public void push(Iterable<PushTask<SkeletonBTreeMap<K, V>.SkeletonNode>> tasks) throws TaskAbortException {
		for (PushTask<SkeletonBTreeMap<K, V>.SkeletonNode> task : tasks) {
			push(task);
		}
	}

	@Override
	public <E> ObjectProcessor<PullTask<SkeletonBTreeMap<K, V>.SkeletonNode>, E, TaskAbortException> pullSchedule(
			BlockingQueue<PullTask<SkeletonBTreeMap<K, V>.SkeletonNode>> input,
			BlockingQueue<X2<PullTask<SkeletonBTreeMap<K, V>.SkeletonNode>, TaskAbortException>> output,
			Map<PullTask<SkeletonBTreeMap<K, V>.SkeletonNode>, E> deposit) {

		return new ObjectProcessor<PullTask<SkeletonBTreeMap<K, V>.SkeletonNode>, E, TaskAbortException>(input, output,
				deposit, null, Executors.DEFAULT_EXECUTOR, new TaskAbortExceptionConvertor()) {
			@Override
			protected Runnable createJobFor(final PullTask<SkeletonBTreeMap<K, V>.SkeletonNode> task) {
				return new Runnable() {
					@Override
					public void run() {
						TaskAbortException ex = null;
						try {
							pull(task);
						} catch (TaskAbortException e) {
							ex = e;
						} catch (RuntimeException e) {
							ex = new TaskAbortException("pull failed", e);
						}
						postProcess.invoke(X2(task, ex));
					}
				};
			}
		}.autostart();

	}

	@Override
	public <E> ObjectProcessor<PushTask<SkeletonBTreeMap<K, V>.SkeletonNode>, E, TaskAbortException> pushSchedule(
			BlockingQueue<PushTask<SkeletonBTreeMap<K, V>.SkeletonNode>> input,
			BlockingQueue<X2<PushTask<SkeletonBTreeMap<K, V>.SkeletonNode>, TaskAbortException>> output,
			Map<PushTask<SkeletonBTreeMap<K, V>.SkeletonNode>, E> deposit) {
		ObjectProcessor<PushTask<SkeletonBTreeMap<K, V>.SkeletonNode>, E, TaskAbortException> objectProcessor = new ObjectProcessor<PushTask<SkeletonBTreeMap<K, V>.SkeletonNode>, E, TaskAbortException>(
				input, output, deposit, null, Executors.DEFAULT_EXECUTOR, new TaskAbortExceptionConvertor()) {
			@Override
			protected Runnable createJobFor(final PushTask<SkeletonBTreeMap<K, V>.SkeletonNode> task) {
				return new Runnable() {
					@Override
					public void run() {
						// Simulate push.
						TaskAbortException ex = null;
						try {
							push(task);
						} catch (TaskAbortException e) {
							ex = e;
						} catch (RuntimeException e) {
							ex = new TaskAbortException("push failed", e);
						}
						postProcess.invoke(X2(task, ex));
					}
				};
			}
		};
		return objectProcessor.autostart();
	}

}