package common.scalegate;

public class TuplesFromAll {

	Object waiter;

	volatile boolean gotFirstTupleFrom[];
	volatile boolean gotTupleFromAll;
	volatile boolean proceed;

	public TuplesFromAll() {
		waiter = new Object();
		proceed = false;
	}

	public void receivedTupleFrom(int input) {
		if (!gotTupleFromAll) {
			if (!gotFirstTupleFrom[input]) {
				gotFirstTupleFrom[input] = true;

				boolean gotfromall = true;
				for (int i = 0; i < gotFirstTupleFrom.length; i++) {
					if (!gotFirstTupleFrom[i])
						gotfromall = false;
				}

				if (gotfromall) {
					synchronized (waiter) {
						gotTupleFromAll = true;
						waiter.notifyAll();
					}
				}
			}
		}
	}

	public void setSize(int size) {
		gotFirstTupleFrom = new boolean[size];
	}

	public boolean receivedTupleFromEachInput() {
		if (proceed) {
			return true;
		} else {
			boolean temp = true;
			for (int i = 0; i < gotFirstTupleFrom.length; i++) {
				temp &= gotFirstTupleFrom[i];
			}
			proceed = temp;
			return proceed;
		}
	}

}
