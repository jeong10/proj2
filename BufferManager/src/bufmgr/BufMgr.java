package bufmgr;

import global.Convert;
import global.GlobalConst;
import global.Minibase;
import global.Page;
import global.PageId;

import java.io.IOException;
import chainexception.ChainException;

import java.util.*;


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

	descriptor[] bufDescr;		// descriptor of frames
	bucket[] directory;				// hash table of pageId with frame number
	Page[] pageData;

	PageId[] replace;					// replace candidates
	float[] crf;							// CRF value of each page
	int[][] ref;							// list of referenced time of each page

	int ref_size = 1024;
	int time = 0;

	int numbufs;
	int numUnpinned;

	int htsize;								// variables for hash table
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

		replacementPolicy = "LRFU";


		bufDescr = new descriptor[numbufs];
		replace = new PageId[numbufs];
		crf = new float[numbufs];
		ref = new int[numbufs][ref_size];

		for (int i=0; i<numbufs; i++) {
			bufDescr[i] = new descriptor();
			bufDescr[i].page_number = new PageId();
			bufDescr[i].pin_count = 0;
			bufDescr[i].dirtybit = false;

			replace[i] = new PageId();

			crf[i] = -1.0f;

			for (int j=0; j<ref_size; j++) {
				ref[i][j] = -1;
			}
		}
	};


	/*
		pin a page.
	*/
	public void pinPage(PageId pageno, Page page, boolean emptyPage)
		throws ChainException, IOException, BufferPoolExceededException, InvalidPageNumberException {

if (f==true && ff==true){

			// throw exception if pid is invalid
			if (pageno.pid < 0 || pageno.pid > htsize) {
				throw new InvalidPageNumberException (null, "bufmgr.InvalidPageNumberException");
			}

			// throw exception if all pages were already pinned
			// and a new page does not exist in the buffer pool
			boolean allPinned = true;
			for (int i=0; i<numbufs; i++) {
				if (bufDescr[i].pin_count == 0) {
					allPinned = false;
					break;
				}
			}
			if (allPinned) {
				boolean notExistingPin = false;
				for (int j=0; j<numbufs; j++) {
					if (bufDescr[j].page_number.pid == pageno.pid) {
						notExistingPin = true;
						break;
					}
				}
				if (notExistingPin == false) {
					throw new BufferPoolExceededException (null, "bufmgr.BufferPoolExceededException");
				}
			}


			int hashIndex = hash(pageno);
			int frameIndex = directory[hashIndex].frame_number;

			// if page in buffer pool, increment pin_count
			if (frameIndex != -1) {

				// if pin_count was 0, remove from replace candidates
				if (bufDescr[frameIndex].pin_count == 0) {
					numUnpinned--;
					replace[frameIndex] = new PageId();
				}
				
				page.copyPage(pageData[pageno.pid]);
				bufDescr[frameIndex].pin_count++;

				directory[hashIndex].page_number = pageno;

				// add reference time
				for (int i=0; i<ref_size; i++) {

					if (ref[frameIndex][i] == -1) {
						ref[frameIndex][i] = time;
						break;
					}
				}
			}

			// otherwise, replace a frame by this page
			else {
				numUnpinned--;

				// calculate CRF for each page frame
				for (int i=0;i<numbufs;i++) {
					crf[i] = -1.0f;
				}

				for (int i=0;i<numbufs;i++) {
					PageId currPage = bufDescr[i].page_number;

					// this frame is holding a page
					if (currPage.pid != -1) {
						crf[i] = 0.0f;

						for (int j=0; j<ref_size; j++) {
							if (ref[i][j] != -1) {
								crf[i] += 1.0f/(time -ref[i][j] +1.0f);
							}
						}
					}
				}

				// calculate min CRF
				float min = Float.MAX_VALUE;
				int frameIndexToReplace = -1;
				PageId pageToReplace = new PageId();

				for (int i=0;i<numbufs;i++) {
					PageId currPage = bufDescr[i].page_number;

					if (currPage.pid != -1) {
						if (crf[i] < min) {
							min = crf[i];
							frameIndexToReplace = i;
							pageToReplace = currPage;
						}
					}

					// if any empty frame exists, pick this frame
					else {
						frameIndexToReplace = i;
						pageToReplace = new PageId();
						break;
					}
				}

				// update the directory
				directory[hashIndex].page_number = pageno;
				directory[hashIndex].frame_number = frameIndexToReplace;

				// if the page on this frame was dirty, flush page to disk
				if (bufDescr[frameIndexToReplace].dirtybit == true) {
					flushPage(bufDescr[frameIndexToReplace].page_number);
				}

				// for the replaced page, remove the reference to this frame
				if (pageToReplace.pid != -1) {
						int h = hash(pageToReplace);
						directory[h].frame_number = -1;
				}

				// update this frame's description
				PageId pid = new PageId(pageno.pid);
				bufDescr[frameIndexToReplace].page_number = pid;
				bufDescr[frameIndexToReplace].pin_count = 1;
				bufDescr[frameIndexToReplace].dirtybit = false;

				// update references
				for (int i=0; i<ref_size; i++) {
					ref[frameIndexToReplace][i] = -1;
				}
				ref[frameIndexToReplace][0] = time;

				// remove new page from replace candidates
				replace[frameIndexToReplace] = new PageId();

				// read/write page content
				Minibase.DiskManager.read_page(pageno, page);

				pageData[pageno.pid] = page;
			}

			time++;
		}
	};


	/*
		unpin a page.
	*/
	public void unpinPage(PageId pageno, boolean dirty) 
		throws IOException, PageUnpinnedException, HashEntryNotFoundException {

if (f==true && ff == true){

			// find frame number
			int hashIndex = hash(pageno);
			PageId page_id = directory[hashIndex].page_number;
			int frameIndex = directory[hashIndex].frame_number;

			// throw exception when this page is not in the buffer pool
			if (directory[hashIndex].page_number.pid == -1) {
				throw new HashEntryNotFoundException (null, "bufmgr: HashEntryNotFoundException.");
			}

			// check if this page is in buffer pool
			if (page_id == pageno && frameIndex != -1) {

				// throw exception when try to unpin a page that is not pinned
				if (bufDescr[frameIndex].pin_count == 0) {
					throw new PageUnpinnedException (null, "bufmgr: PageUnpinnedException.");
				}

				// set dirty bit if page is already modified
				if (dirty == true) {
					bufDescr[frameIndex].dirtybit = true;
					Page p = new Page();
					p.copyPage(pageData[pageno.pid]);
					pageData[pageno.pid] = p;
				}

				bufDescr[frameIndex].pin_count--;

				// add this page to replace candidates if decrementing pin_count results in 0
				if (bufDescr[frameIndex].pin_count == 0) {
					numUnpinned++;
					replace[frameIndex] = pageno;
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
		htsize = getPrime(howmany);
		directory = new bucket[htsize];

		for (int i=0; i<directory.length; i++) {
			directory[i] = new bucket();
			directory[i].page_number = new PageId();
			directory[i].frame_number = -1;
		}

		// initialize time
		time++;

		pageData = new Page[howmany];
		for (int i=0;i<howmany;i++){
			pageData[i] = new Page();
		}

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

					crf[frameIndex] = -1.0f;
					for (int i=0; i<ref_size; i++) {
						ref[frameIndex][i] = -1;
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
			if (pageid.pid < 0 || pageid.pid > htsize) {
				throw new InvalidPageNumberException (null, "bufmgr.InvalidPageNumberException");
			}

			// get frame number
			int hashIndex = hash(pageid);
			PageId page_id = directory[hashIndex].page_number;
			int frameIndex = directory[hashIndex].frame_number;

			// find page in buffer pool
			if (frameIndex != -1) {

				// write page content
ff=false;
					Minibase.DiskManager.write_page(pageid, pageData[pageid.pid]);
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
		return numUnpinned;
	};

	/*
		hashing function.
	*/
	public int hash(PageId pageno) {
		return (a*pageno.pid + b) % htsize;
	}

	/*
		pick min prime number >= howmany
	*/
	public int getPrime(int howmany) {
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
				return z;
			}

			z++;
		}
	};
}
