package org.xnio.nio;

import org.xnio.XnioWorker;

public class WorkerThreadPrioritySetter {

	public static void setAcceptThreadPriority(XnioWorker thread, int priority) {
		final NioXnioWorker worker = (NioXnioWorker)thread;
		WorkerThread acceptThread = worker.getAcceptThread();
		acceptThread.setPriority(priority);
	}
}