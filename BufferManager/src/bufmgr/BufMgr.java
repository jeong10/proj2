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
	class descriptor {
		PageId page_number;
		int pin_count;
		boolean dirtybit;
	};

	//	entry for hash table
	class bucket {
		PageId page_number;
		int frame_number;
	};

	byte[] bufData;						// holds frame contents
	descriptor[] bufDescr;		// descriptor of frames
	bucket[] directory;				// hash table of pageId with frame number

	PageId[] replace;					// replace candidates
	float[] crf;							// CRF value of each page
	int[][] ref;							// list of referenced time of each page

	int numbufs;
	int numpages;
	int numUnpinned;

	int ref_size = 1024;
	int time;

	int htsize;
	int a = 16;
	int b = 24;

	boolean f;
	boolean ff;


	/*
		Constructor.
	*/
	public BufMgr(int numbufs, int lookAheadSize, String replacementPolicy) {
f=false;
ff=false;
		this.numbufs = numbufs;
		numUnpinned = numbufs;

		// set replacement policy
		replacementPolicy = "LRFU";

		// initialize descriptior
		bufDescr = new descriptor[numbufs];

		for (int i=0; i<bufDescr.length; i++) {
			bufDescr[i] = new descriptor();
			bufDescr[i].page_number = new PageId();
			bufDescr[i].pin_count = 0;
			bufDescr[i].dirtybit = false;
		}

		// initialize empty frame contents
		bufData = new byte[GlobalConst.PAGE_SIZE * numbufs];

		for (int i=0; i<bufData.length; i++) {
			bufData[i] = 0;
		}
	};


	/*
		pin a page.
	*/
	public void pinPage(PageId pageno, Page page, boolean emptyPage)
		throws ChainException, IOException, BufferPoolExceededException, InvalidPageNumberException {

if (f==true && ff==true){
			// throw exception if pid is invalid
			if (numpages > 0) {
				if (pageno.pid < 0 || pageno.pid > numpages) {
					throw new InvalidPageNumberException (null, "bufmgr.InvalidPageNumberException");
				}
			}

			// throw exception if all pages were already pinned
			boolean allPinned = true;
			for (int i=0; i<bufDescr.length; i++) {
				if (bufDescr[i].pin_count == 0) {
					allPinned = false;
					break;
				}
			}
			if (allPinned == true) {
				throw new BufferPoolExceededException (null, "bufmgr.BufferPoolExceededException");
			}


			// find frame number
			int hashIndex = hash(pageno);
			int frameIndex = directory[hashIndex].frame_number;

			// if page in buffer pool, increment pin_count
			if (directory[hashIndex].page_number == pageno && frameIndex != -1) {

				// if pin_count was 0, remove from replace candidates
				if (bufDescr[frameIndex].pin_count == 0) {
					numUnpinned--;
					replace[hashIndex] = new PageId();
				}

				bufDescr[frameIndex].pin_count++;

				// add reference time
				for (int i=0; i<ref_size; i++) {
					if (ref[hashIndex][i] == -1) {
						ref[hashIndex][i] = time;
						break;
					}
				}
			}

			// otherwise, replace a frame by this page
			else {
numUnpinned--;

				// calculate CRF for each page frame
				for (int i=0;i<htsize;i++) {
					crf[i]=-1.0f;
				}

				for (int i=0;i<numbufs;i++) {
					PageId currPage = bufDescr[i].page_number;

					// this frame is holding a page
					if (currPage.pid != -1) {
						int hashed = hash(currPage);
						crf[hashed] = 0.0f;
						for (int j=0; j<ref_size; j++) {
							if (ref[hashed][j] != -1) {
/*
* buggy spot. The references do not seemed to be stored
* where it supposed to be.
*/
								crf[hashed] += 1.0f/(time -ref[hashed][j] +1.0f);
							}
						}
					}
				}

				// calculate min CRF
				float min = 999.9f;
				int frameIndexToReplace = -1;
				int hashedPageIndexToReplace = -1;

				for (int i=0;i<numbufs;i++) {
					PageId currPage = bufDescr[i].page_number;

					if (currPage.pid != -1) {
					int hashed = hash(currPage);
						if (crf[hashed] < min) {
							min = crf[hashed];
							frameIndexToReplace = i;
							hashedPageIndexToReplace = hashed;
						}
					}

					// if any empty frame exists, pick this frame
					else {
						if (-1.0f < min) {
							min = -1.0f;
							frameIndexToReplace = i;
							hashedPageIndexToReplace = -1;
						}
					}
				}

				// if the page on this frame was dirty, flush page to disk
				if (bufDescr[frameIndexToReplace].page_number.pid != -1) {
					if (bufDescr[frameIndexToReplace].dirtybit == true) {
						flushPage(bufDescr[frameIndexToReplace].page_number);
					}
				}

				// update the directory
				directory[hashIndex].page_number = pageno;
				directory[hashIndex].frame_number = frameIndexToReplace;

if (hashedPageIndexToReplace != -1) {

				// clear hash entry
				directory[hashedPageIndexToReplace].page_number = new PageId();
				directory[hashedPageIndexToReplace].frame_number = -1;

				// clear previous reference
				for (int i=0; i<ref_size; i++) {
					ref[hashedPageIndexToReplace][i] = -1;
				}

				// remove previous page from replace candidates
				replace[hashedPageIndexToReplace] = new PageId();
}

//System.out.print("R: "+pageno.pid + " at " + frameIndexToReplace+"; ");

				// update this frame's description
				bufDescr[frameIndexToReplace].page_number = pageno;
				bufDescr[frameIndexToReplace].pin_count = 1;
				bufDescr[frameIndexToReplace].dirtybit = false;

				// update references
				for (int i=0; i<ref_size; i++) {
					ref[hashIndex][i] = -1;
				}
				ref[hashIndex][0] = time;

				// remove new page from replace candidates
				replace[hashIndex] = new PageId();

				// read/write page content
				Minibase.DiskManager.read_page(pageno, page);

				byte[] data = page.getData();
				for (int j=0; j<GlobalConst.PAGE_SIZE; j++) {
					bufData[GlobalConst.PAGE_SIZE*frameIndexToReplace +j] = data[j];
				}
			}

			time++;
		}
	};


	/*
		unpin a page.
	*/
	public void unpinPage(PageId pageno, boolean dirty) 
		throws PageUnpinnedException {

if (f==true && ff == true){

			// find frame number
			int hashIndex = hash(pageno);
			PageId page_id = directory[hashIndex].page_number;
			int frameIndex = directory[hashIndex].frame_number;

			// check if this page is in buffer pool
			if (page_id == pageno && frameIndex > -1) {

				// throw exception when try to unpin a page that is not pinned
				if (bufDescr[frameIndex].pin_count == 0) {
					throw new PageUnpinnedException (null, "bufmgr: PageUnpinnedException.");
				}

				// set dirty bit if page is already modified
				if (dirty == true) {
					bufDescr[frameIndex].dirtybit = true;
				}

				bufDescr[frameIndex].pin_count--;

				// add this page to replace candidates if decrementing pin_count results in 0
				if (bufDescr[frameIndex].pin_count == 0) {
					numUnpinned++;
					replace[hashIndex] = pageno;
				}
			}
		}
	};


	/*
		allocate a set of page(s).
	*/
	public PageId newPage(Page firstpage, int howmany)
		throws ChainException, IOException {

		// initialize directory
		// pick min prime number >= howmany
		int minPrime = 0;
		int z = howmany;

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
		htsize = minPrime;
		directory = new bucket[htsize];

		for (int i=0; i<directory.length; i++) {
			directory[i] = new bucket();
			directory[i].page_number = new PageId();
			directory[i].frame_number = -1;
		}

		numpages = htsize;

		// initialize replacement candidates
		replace = new PageId[htsize];
		for (int i=0; i<replace.length; i++) {
			replace[i] = new PageId();
		}

		// initialize CRF
		crf = new float[htsize];
		for (int i=0; i<htsize; i++) {
			crf[i] = -1.0f;
		}

		// initialize ref
		ref = new int[htsize][ref_size];
		for (int i=0; i<htsize; i++) {
			for (int j=0; j<ref_size; j++) {
				ref[i][j] = -1;
			}
		}

		// initialize time
		time = 1;

		// allocate pages to disk
ff=false;
		PageId pageno = Minibase.DiskManager.allocate_page(howmany);
ff=true;

		// check if buffer pool is full
		boolean isNotFull = true;
		for (int i=0; i<bufDescr.length; i++) {
			if (bufDescr[i].page_number.pid != -1) {
				isNotFull = false;
				break;
			}
		}
		if (isNotFull = false) {
ff=false;
			Minibase.DiskManager.deallocate_page(pageno, howmany);
ff=true;
			return null;
		}

		// check if page is already allocated
		for (int i=0; i<bufDescr.length; i++) {
			if (bufDescr[i].page_number == pageno) {
ff=false;
				Minibase.DiskManager.deallocate_page(pageno, howmany);
ff=true;
				return null;
			}
		}

		// if not, find a frame in the buffer pool
		for (int i=0; i<bufDescr.length; i++) {
			if (bufDescr[i].page_number.pid == -1) {
f=true;
ff=true;
				pinPage(pageno, firstpage, false);
				Minibase.DiskManager.write_page(pageno, firstpage);

				return pageno;
			}
		}

		return null;
	};


	/*
		free a single page.
	*/
	public void freePage(PageId globalPageId)
		throws ChainException, PagePinnedException {

			if (globalPageId.pid > -1 && globalPageId.pid < htsize) {

				// find frame index				
				int hashIndex = hash(globalPageId);
				PageId page_id = directory[hashIndex].page_number;
				int frameIndex = directory[hashIndex].frame_number;

				// throw exception when this page is already pinned
				if (page_id == globalPageId && frameIndex != -1) {
					if (bufDescr[frameIndex].pin_count > 0) {
						throw new PagePinnedException(null, "bufmgr.PagePinnedException");
					}

					// clear info
					bufDescr[frameIndex].page_number = new PageId();
					bufDescr[frameIndex].pin_count = 0;
					bufDescr[frameIndex].dirtybit = false;

					directory[hashIndex].page_number = new PageId();
					directory[hashIndex].frame_number = -1;

					crf[hashIndex] = -1.0f;
					for (int i=0; i<ref_size; i++) {
						ref[hashIndex][i] = -1;
					}

ff=false;
					Minibase.DiskManager.deallocate_page(globalPageId);
ff=true;
				}
			}
	};

	/*
		flush a single page.
	*/
	public void flushPage(PageId pageid) 
		throws ChainException, IOException, InvalidPageNumberException {

			// throw exception if pid is invalid
			if (pageid.pid < 0 || pageid.pid > numpages) {
				throw new InvalidPageNumberException (null, "bufmgr.InvalidPageNumberException");
			}

			// get frame number
			int hash_index = hash(pageid);
			PageId page_id = directory[hash_index].page_number;
			int frame_index = directory[hash_index].frame_number;

			// find page in buffer pool
			if (page_id == pageid && frame_index > -1) {

				// write page content
				byte[] data = new byte[GlobalConst.PAGE_SIZE];
				for (int i=0; i<GlobalConst.PAGE_SIZE; i++) {
					data[i] = bufData[GlobalConst.PAGE_SIZE*frame_index +i];
				}

				Page page = new Page(data);
ff=false;
				Minibase.DiskManager.write_page(pageid, page);
ff=true;
			}
	};


	/*
		flush all pages.
	*/
	public void flushAllPages()
		throws ChainException, IOException {
			for (int i=0; i<numbufs; i++) {
				if (bufDescr[i].page_number.pid != -1) {
					flushPage(bufDescr[i].page_number);
				}
			}
	};

	/*
		returns the number of buffers.
	*/
	public int getNumBuffers() {
		return numbufs;
	};

	/*
		returns the number of unpinned pages (in frames).
	*/
	public int getNumUnpinned() {
//		return numUnpinned;

int count=0;
for(int i=0;i<numbufs;i++){
if(bufDescr[i].pin_count==0){
count++;
}
}
return count;
	};

	/*
		Hashing function.
	*/
	public int hash(PageId pageno) {
		return (a*pageno.pid + b) % htsize;
	}
}
