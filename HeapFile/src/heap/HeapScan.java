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
	boolean first;

	int scannedRecord;
	int scanRemaining;

	RID currRid;
	RID nextRid;

	/*
		Constructor.
	*/
	protected HeapScan(HeapFile hf) {
		this.hf = hf;
		first = true;

		scannedRecord = 0;
		scanRemaining = hf.getRecCnt();

		currRid = new RID();
		nextRid = null;

		// pin directory header page
		Minibase.BufferManager.pinPage(hf.getHeader(), hf.getHeaderPage(), false);
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

		boolean isNoneLeft = true;

		if (scanRemaining == 0)
			isNoneLeft = false;

		return isNoneLeft;
	};


	/*
		get next record.
	*/
	public Tuple getNext(RID rid) {

		// scanned all; unpin and return
		if (scanRemaining == 0) {

			scannedRecord = 0;
			scanRemaining = hf.getRecCnt();

			Minibase.BufferManager.unpinPage(hf.getHeader(), false);

			return null;
		}

		HFPage hfp;

		// set up variables if first time
		if (first == true) {
			first = false;
			currRid = rid;

			// if rid is empty, point to the very first record
			hfp = hf.dir[0].pageLocation[0];
			nextRid = hfp.firstRecord();

			int hashIndex = hf.hash(nextRid);
			currRid = nextRid;
			rid.copyRID(nextRid);

//System.out.println("record: " + rid.pageno.pid + ", " + rid.slotno +
//	" with " + scannedRecord+ " scanned");

			scannedRecord++;
			scanRemaining--;

			return hf.getRecords()[hashIndex].tuple;
		}

		int pageLocation = currRid.pageno.pid;
		int dirLocation = pageLocation / hf.getNumDir();
		hfp = hf.dir[dirLocation].pageLocation[pageLocation];

		nextRid = hfp.nextRecord(currRid);
		if (nextRid == null) {
			
			// rid is the last record of this HFPage
			// search for next HFPage within this directory
			// if not found, search for next directory
			boolean found = false;
			for (int d=dirLocation; d<hf.getNumDir(); d++){
				for (int i=pageLocation+1; i<hf.getNumPages(); i++) {
					HFPage currHFP = hf.dir[d].pageLocation[i];

					if (currHFP != null) {
						RID temp = currHFP.firstRecord();
						if (temp != null) {
							found = true;
							nextRid = temp;
							break;
						}
					}
				}
			}

			// search from the beginning
			if (found == false) {

				for (int d=0; d<=dirLocation; d++) {
					for (int i=0; i<hf.getNumPages(); i++) {
						HFPage currHFP = hf.dir[d].pageLocation[i];

						if (currHFP != null) {
							RID temp = currHFP.firstRecord();

							// the rid found is the same as the given rid,
							// we made a full round of scanning; return null
							if (temp != null) {
								if (temp == rid) {
									return null;
								}
								else {
									found = true;
									nextRid = temp;
									break;
								}
							}
						}
					}
				}
			}
		}

		int hashIndex = hf.hash(nextRid);
		currRid = nextRid;
		rid.copyRID(nextRid);

		scannedRecord++;
		scanRemaining--;

//		System.out.println("record: " + rid.pageno.pid + ", " + rid.slotno +
//		" with " + scannedRecord+ " scanned");

		return hf.getRecords()[hashIndex].tuple;
	}


	/*
		close the scan.
	*/
	public void close()
		throws ChainException {
			first = true;
			scannedRecord = hf.getRecCnt() - scanRemaining;
	};
}
