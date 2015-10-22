package heap;

import global.Convert;
import global.GlobalConst;
import global.Minibase;
import global.RID;
import global.Page;
import global.PageId;

import java.io.IOException;

import chainexception.ChainException;


/*
	HeapScan class.
*/
public class HeapScan {

	HeapFile hf;

	/*
		Constructor.
	*/
	protected HeapScan(HeapFile hf) {
		this.hf = hf;

		// pin directory header page
		Minibase.BufferManager.pinPage(hf.getHeader(), hf.getHeaderPage(), false);

		// initialize iterator fields
	};

	
	/*
		close HeapScan if it's open.
	*/
	protected void finalize()
		throws Throwable {
//System.out.println("finalize");
	};


	/*
		check if more records left to scan.
	*/
	public boolean hasNext() {

		boolean none = false;

		return none;
	};


	/*
		get next record.
	*/
	public Tuple getNext(RID rid) {

		// find the record
		PageId pageno = rid.pageno;
		int slotno = rid.slotno;

		int dirIndex = pageno.pid / hf.numDir;
		int pageIndex = pageno.pid % hf.numDir;
//		HFPage hfp = hf.dir[dirIndex].pageLocation[pageIndex];

//		boolean next = hfp.hasNext(rid);
		boolean next = false;
		if (next == false) {
			Minibase.BufferManager.unpinPage(hf.getHeader(), false);
		}

		return null;
	}


	/*
		unpin any pinned pages.
	*/
	public void close()
		throws ChainException {
//System.out.println("closing");
	};
}
