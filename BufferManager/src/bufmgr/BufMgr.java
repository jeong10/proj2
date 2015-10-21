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

	boolean f = false;
	boolean ff = false;


	/*
		Constructor.
	*/
	public BufMgr(int numbufs, int lookAheadSize, String replacementPolicy) {

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

		this.numbufs = numbufs;
		numUnpinned = numbufs;


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
		if (f == true && ff == true) {

			// throw exception if pid is invalid
			if (pageno.pid < 0 || pageno.pid > numpages) {
				throw new InvalidPageNumberException (null, "bufmgr.InvalidPageNumberException");
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
			int hash_index = hash(pageno);
			directory[hash_index].page_number = pageno;
			int frame_index = directory[hash_index].frame_number;

			// if page in buffer pool, increment pin_count
			if (frame_index > -1) {

				// if pin_count was 0, remove from replace candidates
				if (bufDescr[frame_index].pin_count == 0) {
					numUnpinned--;
					replace[hash_index] = new PageId();e
				}

				bufDescr[frame_index].pin_count++;

				// add reference time
				for (int i=0; i<ref_size; i++) {
					if (ref[hash_index][i] == -1) {
						ref[hash_index][i] = time;
						break;
					}
				}
			}

			// otherwise, replace a frame by this page
			else {

				numUnpinned--;

				// calculate CRF for each page
				for (int i=0; i<numpages; i++) {
					crf[i] = 0.0f;

					for (int j=0; j<ref_size; j++) {
						if (ref[i][j] > 0) {
							crf[i] += 1.0f/(time -ref[i][j] +1.0f);
						}
					}
				}

				// obtain a page with min CRF
				float min = crf[0];
				int page_replace = 0;

				for (int i=0; i<numpages; i++) {
					if (crf[i] < min) {
						min = crf[i];
						page_replace = i;
					}
				}

				int frame_index_replace = directory[page_replace].frame_number;

				// this frame was holding a page
				if (frame_index_replace != -1) {

					// if dirtybit = 1, flush page to disk
					if (bufDescr[frame_index_replace].dirtybit == true) {
						flushPage(bufDescr[frame_index_replace].page_number);
					}

					// clear hash entry
					directory[page_replace].page_number = new PageId();
					directory[page_replace].frame_number = -1;

					// clear references
					for (int j=0; j<ref_size; j++) {
						ref[page_replace][j] = -1;
					}

					// update the directory
					directory[hash_index].page_number = pageno;
					directory[hash_index].frame_number = frame_index_replace;

					// update this frame's description
					bufDescr[frame_index_replace].page_number = pageno;
					bufDescr[frame_index_replace].pin_count = 1;
					bufDescr[frame_index_replace].dirtybit = false;

					// read page content
					Minibase.DiskManager.read_page(pageno, page);

					// write page content to this frame
					byte[] data = page.getData();

					for (int j=0; j<GlobalConst.PAGE_SIZE; j++) {
						bufData[GlobalConst.PAGE_SIZE*frame_index_replace +j] = data[j];
					}
				}

				// otherwise, pick an empty frame
				else {

					for (int i=0; i<numbufs; i++) {
						if (bufDescr[i].page_number.pid == -1) {

							// update the directory
							directory[hash_index].page_number = pageno;
							directory[hash_index].frame_number = i;

							// update this frame's description
							bufDescr[i].page_number = pageno;
							bufDescr[i].pin_count = 1;
							bufDescr[i].dirtybit = false;

							// read page content
							Minibase.DiskManager.read_page(pageno, page);

							// write page content to this frame
							byte[] data = page.getData();

							for (int j=0; j<GlobalConst.PAGE_SIZE; j++) {
								bufData[GlobalConst.PAGE_SIZE*i +j] = data[j];
							}

							break;
						}
					}
				}

				// update references
				for (int i=0; i<ref_size; i++) {
					ref[hash_index][i] = -1;
				}
				ref[hash_index][0] = time;

				// remove this page from replace candidates
				replace[hash_index] = new PageId();

				time++;
			}
		}
	};


	/*
		unpin a page.
	*/
	public void unpinPage(PageId pageno, boolean dirty) 
		throws PageUnpinnedException {
		if (f == true && ff == true) {

			// find frame number
			int hash_index = hash(pageno);
			PageId page_id = directory[hash_index].page_number;
			int frame_index = directory[hash_index].frame_number;

			// check if this page is in buffer pool
			if (page_id == pageno && frame_index > -1) {

				// throw exception when try to unpin a page that is not pinned
				if (bufDescr[frame_index].pin_count == 0) {
					throw new PageUnpinnedException (null, "bufmgr: PageUnpinnedException.");
				}

				// set dirty bit if page is already modified
				if (dirty == true) {
					bufDescr[frame_index].dirtybit = true;
				}

				bufDescr[frame_index].pin_count--;

				// add this page to replace candidates if decrementing pin_count results in 0
				if (bufDescr[frame_index].pin_count == 0) {
					numUnpinned++;
					replace[hash_index] = pageno;
				}
			}
		}
	};


	/*
		allocate a set of page(s).
	*/
	public PageId newPage(Page firstpage, int howmany)
		throws ChainException, IOException {
//System.out.println(":new");

		f = true;

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
		replace = new PageId[numpages];
		for (int i=0; i<replace.length; i++) {
			replace[i] = new PageId();
		}

		// initialize CRF
		crf = new float[numpages];
		for (int i=0; i<crf.length; i++) {
			crf[i] = -1.0f;
		}

		// initialize ref
		ref = new int[numpages][ref_size];
		for (int i=0; i<ref.length; i++) {
			for (int j=0; j<ref_size; j++) {
				ref[i][j] = -1;
			}
		}

		// initialize time
		time = 1;


		// allocate pages to disk
		ff = false;
		PageId pageno = Minibase.DiskManager.allocate_page(howmany);
		ff = true;

		// check if buffer pool is full
		boolean isNotFull = true;
		for (int i=0; i<bufDescr.length; i++) {
			if (bufDescr[i].page_number.pid > -1) {
				isNotFull = false;
				break;
			}
		}
		if (isNotFull = false) {
			ff = false;
			Minibase.DiskManager.deallocate_page(pageno, howmany);
			ff = true;
			return null;
		}

		// check if page is already allocated
		for (int i=0; i<bufDescr.length; i++) {
			if (bufDescr[i].page_number == pageno) {
				ff = false;
				Minibase.DiskManager.deallocate_page(pageno, howmany);
				ff = true;
				return null;
			}
		}

		// if not, find a frame in the buffer pool
		for (int i=0; i<bufDescr.length; i++) {
			if (bufDescr[i].page_number.pid == -1) {

				// update descriptor
				bufDescr[i].page_number = pageno;
				bufDescr[i].pin_count = 1;
				bufDescr[i].dirtybit = false;

				// update hash table
				int hash_index = hash(pageno);
				directory[hash_index].page_number = pageno;
				directory[hash_index].frame_number = i;

				// add page content to buffer
				byte[] data = firstpage.getData();

				for (int j=0; j<GlobalConst.PAGE_SIZE; j++) {
					bufData[GlobalConst.PAGE_SIZE*i +j] = data[j];
				}

				Minibase.DiskManager.write_page(pageno, firstpage);

				break;
			}
		}

		return pageno;
	};

	/*
		free a single page.
	*/
	public void freePage(PageId globalPageId)
		throws ChainException, PagePinnedException {

			if (globalPageId.pid > -1) {

				// throw exception when this page is already pinned
				int hash_index = hash(globalPageId);
				PageId page_id = directory[hash_index].page_number;
				int frame_index = directory[hash_index].frame_number;

				if (page_id == globalPageId && frame_index > -1) {
					if (bufDescr[frame_index].pin_count > 0) {
						throw new PagePinnedException(null, "bufmgr.PagePinnedException");
					}
				}

				ff = false;
				Minibase.DiskManager.deallocate_page(globalPageId);
				ff = true;
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
				Minibase.DiskManager.write_page(pageid, page);
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
		return numUnpinned;
	};

	/*
		Hashing function.
	*/
	public int hash(PageId pageno) {
		return (a*pageno.pid + b) % htsize;
	}
}
