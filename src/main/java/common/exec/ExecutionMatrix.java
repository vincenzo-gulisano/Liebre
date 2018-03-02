package common.exec;

//FIXME: Extract common interface
//FIXME: Rename "Tasks" to something more generic
abstract class ExecutionMatrix {

	protected final int nThreads;
	protected final int nTasks;

	protected ExecutionMatrix(int nTasks, int nThreads) {
		this.nThreads = nThreads;
		this.nTasks = nTasks;
	}

	protected int getIndex(int threadId, int taskId) {
		return (nTasks * threadId) + taskId;
	}
}
