package bufmgr;

import global.Convert;
import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.io.IOException;
import chainexception.ChainException;

import java.util.*;

/*
	BufMgr class.
*/
public class BufMgr {

	//	entry for buffer frame descriptors
	private class descriptor {
		public PageId page_number;
		public int pin_count;
		public boolean dirtybit;
	};

	//	entry for hash table of frames
	private class bucket {
		public PageId page_number;
		public int frame_number;
	};

	private byte[] bufPool;					// stores data of pages, as bytes
	private descriptor[] bufDescr;	// descriptor of frames
	private bucket[] directory;			// hash table of pageId and frame number

	private float[] crf;						// crf value for LRFU policy
	private int[][] ref;						// stores when pages are referenced

	private PageId[] replaceCandidates;

	private int numbufs;						// number of buffers


	// variables for hash table
	private int htsize;
	private int a = 2;
	private int b = 3;

	// variables for LRFU policy
	private int numrefs;
	private int time;

	/*
		Constructor.
	*/
	public BufMgr(int numbufs, int lookAheadSize, String replacementPolicy) {

		// pick max prime number < numbufs
		int[] primes = new int[numbufs];
		int index = 0;
		for (int i=0; i<numbufs; i++)
			primes[i] = 0;

		for (int i=0; i<numbufs; i++) {
			boolean isPrime = true;

			for (int j=2; j<i; j++) {
				if (i % j == 0) {
					isPrime = false;
					break;
				}
			}

			if (isPrime) {
				primes[index] = i;
				index++;
			}
		}

		Arrays.sort(primes);

		this.numbufs = numbufs;
		this.htsize = primes[primes.length-1];
		this.time = 1;
		this.numrefs = 1000;
		replacementPolicy = "LRFU";

		bufPool = new byte[GlobalConst.PAGE_SIZE * numbufs];
		bufDescr = new descriptor[numbufs];
		directory = new bucket[htsize];
		crf = new float[numbufs];
		ref = new int[numbufs][numrefs];
		replaceCandidates = new PageId[numbufs];

		for (int i=0; i<bufPool.length; i++) {
			bufPool[i] = 0;
		}

		for (int i=0; i<numbufs; i++) {
			bufDescr[i] = new descriptor();
			bufDescr[i].page_number = null;
			bufDescr[i].pin_count = 0;
			bufDescr[i].dirtybit = false;

			crf[i] = -1.0f;
			for (int j=0; j<numrefs; j++) {
				ref[i][j] = -1;
			}
			replaceCandidates[i] = null;
		}

		for (int i=0; i<htsize; i++) {
			directory[i] = new bucket();
			directory[i].page_number = null;
			directory[i].frame_number = -1;
		}
	};


	/*
		pinPage.
	*/
	public void pinPage(PageId pageno, Page page, boolean emptyPage)
		throws PagePinnedException {
		
			int hash_index = hash(pageno);
			PageId page_id = directory[hash_index].page_number;
			int frame_index = directory[hash_index].frame_number;

			// if page in buffer pool, increment pin_count
			if (pageno == page_id) {
				if (pageno == bufDescr[frame_index].page_number)
					bufDescr[frame_index].pin_count++;

					// add reference time
					for (int i=0; i<numrefs; i++) {
						if (ref[frame_index][i] == -1) {
							ref[frame_index][i] = time;
							break;
						}
					}

					// update CRF value of this page
					crf[frame_index] = 0.0f;

					for (int i=0; i<numrefs; i++) {
						if (ref[frame_index][i] == -1)
							break;
						crf[frame_index] += (1.0f/(time -ref[frame_index][i] +1));
					}
			}

			// replace a frame F (chosen by LRFU policy) by P
			else {

				// pick frame F for replacement
				// F = page (in frame) with min CRF value
				float min = crf[0];
				int frame_index_replace = 0;
				for (int i=0; i<crf.length; i++) {
					if (crf[i] < min) {
						min = crf[i];
						frame_index_replace = i;
					}
				}

				PageId pageIdToReplace = bufDescr[frame_index_replace].page_number;
				int hash_index_replace = hash(pageIdToReplace);

				// if dirtybit=1 flush page to disk
				if (bufDescr[frame_index_replace].dirtybit == true) {
					flushPage(bufDescr[frame_index_replace].page_number);
				}

				// remove any content related to F
				// add P to where F was
				// bufPool[] not yet implemented
				directory[hash_index].page_number = pageno;
				directory[hash_index].frame_number = directory[hash_index_replace].frame_number;

				directory[hash_index_replace].page_number = null;
				directory[hash_index_replace].frame_number = -1;

				bufDescr[frame_index_replace].page_number = pageno;
				bufDescr[frame_index_replace].pin_count = 1;
				bufDescr[frame_index_replace].dirtybit = false;

				// update CRF and reference info
				crf[frame_index_replace] = 1.0f;
				for (int i=0; i<numrefs; i++) {
					ref[frame_index_replace][i] = -1;
				}
				ref[frame_index_replace][0] = time;
			}
			
			time++;
			// if all pages already pinned
			boolean allPinned = true;
			for (int i=0; i<numbufs; i++) {
				if (replaceCandidates[i] != null) {
					int hashIndex = hash(replaceCandidates[i]);
					int frameIndex = directory[hashIndex].frame_number;

					if (bufDescr[frameIndex].pin_count > 0) {
						allPinned = false;
						break;
					}
				}
			}
			if (allPinned) {
				throw new PagePinnedException (null, "bufmgr.PagePinnedException");
			}
	};

	/*
		unpinPage.
	*/
	public void unpinPage(PageId pageno, boolean dirty) 
		throws PageUnpinnedException {

		int hash_index = hash(pageno);
		int frame_index = directory[hash_index].frame_number;

		//	throw exception when try to unpin a page that is not pinned
		if (bufDescr[frame_index].pin_count == 0) {
			throw new PageUnpinnedException (null, "BUFMGR: PAGE_NOT_PINNED.");
		}

		// set dirty bit if page is already modified
		if (dirty == true) {
			bufDescr[frame_index].dirtybit = true;
		}

		// decrement pin_count
		if (bufDescr[frame_index].pin_count > 0) {
			bufDescr[frame_index].pin_count--;
		}
		
		// add pageno to replaceCandidates if decrementing pin_count results in 0
		if (bufDescr[frame_index].pin_count == 0) {
			for (int i=0; i<numbufs; i++) {
				if (replaceCandidates[i] == null) {
					replaceCandidates[i] = pageno;
					break;
				}
			}
		}
	};
	 
	/*
		newPage.
	*/
	public PageId newPage(Page firstpage, int howmany) 
		throws ChainException, IOException {

		PageId pageno = Minibase.DiskManager.allocate_page(howmany);

		int hash_index = hash(pageno);

		// find frame in the buffer pool
		if (directory[hash_index].page_number == null) {
			
			directory[hash_index].page_number = pageno;
			

			pinPage(pageno, firstpage, false);

			return pageno;
		}

		// else (buffer pool is full)
		else {
			Minibase.DiskManager.deallocate_page(pageno, howmany);
			return null;
		}
	};

	public void freePage(PageId globalPageId)
		throws PagePinnedException {
			
			// throw exception when this page is already pinned
			int hash_index = (a*globalPageId.pid +b) % htsize;
			int frame_index = directory[hash_index].frame_number;
			int pinCount = bufDescr[frame_index].pin_count;
			if (pinCount > 0) {
				throw new PagePinnedException(null, "bufmgr.PagePinnedException");
			}
	};

	public void flushPage(PageId pageid) {};

	public void flushAllPages() {};

	public int getNumBuffers() {
		return numbufs;
	}

	public int getNumUnpinned() {
		return 0;
	}

	/*
		Hash function.
	*/
	public int hash(PageId pageno) {
		return (a*pageno.pid + b) % htsize;
	}
};
