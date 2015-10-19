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

	private int[] replaceCandidates;

	private int numbufs;						// number of buffers
	private int numUnpinned;				// number of unpinned pages
	private int p_id;

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

		// pick min prime number > numbufs
		int minPrime = 0;
		int z = numbufs;

		while(true) {
			boolean isPrime = true;

			for (int j=2; j<z; j++) {
				if (z % j == 0) {
					isPrime = false;
					break;
				}
			}

			if (isPrime) {
				minPrime = z;
				break;
			}

			z++;
		}

		this.p_id = -1;
		this.numbufs = numbufs;
		this.numUnpinned = numbufs;
		this.htsize = minPrime;
		this.time = 1;
		this.numrefs = 1000;
		replacementPolicy = "LRFU";

		bufPool = new byte[GlobalConst.PAGE_SIZE * numbufs];
		bufDescr = new descriptor[numbufs];
		directory = new bucket[htsize];
		crf = new float[numbufs];
		ref = new int[numbufs][numrefs];
		replaceCandidates = new int[numbufs];

		for (int i=0; i<bufPool.length; i++) {
			bufPool[i] = 0;
		}

		for (int i=0; i<numbufs; i++) {
			bufDescr[i] = new descriptor();
			bufDescr[i].page_number = new PageId();
			bufDescr[i].pin_count = 0;
			bufDescr[i].dirtybit = false;

			crf[i] = -1.0f;
			for (int j=0; j<numrefs; j++) {
				ref[i][j] = -1;
			}

			replaceCandidates[i] = i;
		}
		for (int i=0; i<htsize; i++) {
			directory[i] = new bucket();
			directory[i].page_number = new PageId();
			directory[i].frame_number = -1;
		}
	};


	/*
		pinPage.
	*/
	public void pinPage(PageId pageno, Page page, boolean emptyPage)
		throws PagePinnedException {

			// throw exception if all pages were already pinned
			if (numUnpinned == 0) {
//System.out.println("page pinned exception here");
				throw new PagePinnedException (null, "bufmgr.PagePinnedException");
			}
		
			int hash_index = hash(pageno);
			PageId page_id = directory[hash_index].page_number;
			int frame_index = directory[hash_index].frame_number;

			// if page in buffer pool, increment pin_count
			if (pageno == page_id && frame_index >= 0) {
				if (pageno == bufDescr[frame_index].page_number)

					// if pin_count was 0, remove from replaceCandidates
					if (bufDescr[frame_index].pin_count == 0) {
						for (int i=0; i<replaceCandidates.length; i++) {
							if (replaceCandidates[i] == bufDescr[frame_index].page_number.pid) {
								replaceCandidates[i] = -1;
								numUnpinned--;
								break;
							}
						}
					}

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
			else {

				// pick F from replaceCandidates
				// F = page (in frame) with min CRF value
				float min = crf[0];
				int frame_index_replace = 0;

				for (int i=0; i<replaceCandidates.length; i++) {
					if (replaceCandidates[i] != -1) {

						for (int j=0; j<numbufs; j++) {
								if (crf[j] < min) {
									min = crf[j];
									frame_index_replace = j;
								}
						}
					}
				}

				// remove any content related to F
				// replace F by P
				PageId pageIdToReplace = bufDescr[frame_index_replace].page_number;

				if (pageIdToReplace != null) {
					int hash_index_replace = hash(pageIdToReplace);

					// if dirtybit=1 flush page to disk
					if (bufDescr[frame_index_replace].dirtybit == true) {
						flushPage(bufDescr[frame_index_replace].page_number);
					}

					directory[hash_index].frame_number = directory[hash_index_replace].frame_number;

					directory[hash_index_replace].page_number = new PageId();
					directory[hash_index_replace].frame_number = -1;


				}
				else {
					directory[hash_index].frame_number = frame_index_replace;
				}
				numUnpinned--;
				directory[hash_index].page_number = pageno;
				
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
		};

	/*
		unpinPage.
	*/
	public void unpinPage(PageId pageno, boolean dirty) 
		throws PageUnpinnedException {

		int hash_index = hash(pageno);
		int frame_index = directory[hash_index].frame_number;

		//	throw exception when try to unpin a page that is not pinned
		if (frame_index > -1) {
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
				numUnpinned++;

				boolean found = false;

				// find if this pageno already exists
				for (int i=0; i<replaceCandidates.length; i++) {
					if (replaceCandidates[i] == pageno.pid) {
						found = true;
						break;
					}
				}

				// if not, add to replaceCandidates
				if (found == false) {
					for (int i=0; i<replaceCandidates.length; i++) {
						if (replaceCandidates[i] == -1) {
							replaceCandidates[i] = pageno.pid;
							
							break;
						}
					}
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
		p_id++;
		pageno.pid = p_id;

		int hash_index = hash(pageno);
		directory[hash_index].page_number = pageno;

		// check if buffer pool is full
		boolean isNotFull = true;
		for (int i=0; i<bufDescr.length; i++) {
			if (bufDescr[i].page_number != null) {
				if (bufDescr[i].page_number.pid > -1) {
					isNotFull = false;
					break;
				}
			}
		}
		if (isNotFull = false) {
System.out.println("buffer full?");
			Minibase.DiskManager.deallocate_page(pageno, howmany);
			return null;
		}

		// if not, find a frame in the buffer pool
		for (int i=0; i<bufDescr.length; i++) {
			if (bufDescr[i].page_number.pid == -1) {
				bufDescr[i].page_number = pageno;
				bufDescr[i].pin_count = 1;
				bufDescr[i].dirtybit = false;

				directory[hash_index].frame_number = i;

				crf[i] = 1.0f;
				ref[i][0] = time;
				time++;

				numUnpinned--;

				// remove from replacement candidates
				for (int j=0; j<replaceCandidates.length; j++) {
					if (replaceCandidates[j] == pageno.pid) {
						replaceCandidates[j] = -1;
					}
				}

				break;
			}
		}

		return pageno;
	};

	/*
		freePage
	*/
	public void freePage(PageId globalPageId)
		throws PagePinnedException {
			
			if (globalPageId.pid > -1) {

				// throw exception when this page is already pinned
				int hash_index = hash(globalPageId);
				int frame_index = directory[hash_index].frame_number;
			
				if (frame_index > -1) {
					if (bufDescr[frame_index].pin_count > 0) {
						throw new PagePinnedException(null, "bufmgr.PagePinnedException");
					}
				}
			}
	};

	public void flushPage(PageId pageid) {};

	public void flushAllPages() {};

	public int getNumBuffers() {
		return numbufs;
	}

	public int getNumUnpinned() {
		return numUnpinned;
	}

	/*
		Hash function.
	*/
	public int hash(PageId pageno) {
		return (a*pageno.pid + b) % htsize;
	}
};
