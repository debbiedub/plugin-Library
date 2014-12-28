/* This code is part of Freenet. It is distributed under the GNU General
 * Public License, version 2 (or at your option any later version). See
 * http://www.gnu.org/ for further details of the GPL. */
package freenet.library.io.serial;

import java.util.Map;
import java.util.concurrent.BlockingQueue;

import freenet.library.io.serial.Serialiser.*;
import freenet.library.util.concurrent.ObjectProcessor;
import freenet.library.util.concurrent.Scheduler;
import freenet.library.util.exec.TaskAbortException;
import freenet.library.util.func.Tuples.X2;

/**
** An interface for asynchronous task execution. The methods return objects
** for managing and scheduling tasks.
**
** @author infinity0
*/
public interface ScheduledSerialiser<T> extends IterableSerialiser<T> {

	/**
	** Creates a {@link ObjectProcessor} for executing {@link PullTask}s, which
	** should be {@linkplain ObjectProcessor#auto()} automatically started.
	**
	** @param input Queue to add task requests to
	** @param output Queue to pop completed tasks from
	** @param deposit Map of tasks to deposits
	*/
	// TODO LOW public <E> Scheduler pullSchedule(
	public <E> ObjectProcessor<PullTask<T>, E, TaskAbortException> pullSchedule(
		BlockingQueue<PullTask<T>> input,
		BlockingQueue<X2<PullTask<T>, TaskAbortException>> output,
		Map<PullTask<T>, E> deposit
	);

	/**
	** Creates a {@link ObjectProcessor} for executing {@link PushTask}s, which
	** should be {@linkplain ObjectProcessor#auto()} automatically started.
	**
	** @param input Queue to add task requests to
	** @param output Queue to pop completed tasks from
	** @param deposit Map of tasks to deposits
	*/
	// TODO LOW public <E> Scheduler pushSchedule(
	public <E> ObjectProcessor<PushTask<T>, E, TaskAbortException> pushSchedule(
		BlockingQueue<PushTask<T>> input,
		BlockingQueue<X2<PushTask<T>, TaskAbortException>> output,
		Map<PushTask<T>, E> deposit
	);

}