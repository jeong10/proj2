package heap;

import global.Convert;
import global.GlobalConst;
import global.Minibase;
import global.RID;

import java.io.IOException;

import chainexception.ChainException;


/*
	HeapScan class.
*/
public class HeapScan {

	HeapFile hf;

	protected HeapScan(HeapFile hf) {
		this.hf = hf;
	};

	protected void finalize()
		throws Throwable {
	};

	public boolean hasNext() {
		return false;
	};

	public Tuple getNext(RID rid) {
		return null;
	}

	public void close()
		throws ChainException {
	};
}
