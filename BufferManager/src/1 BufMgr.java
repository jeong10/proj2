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
		public boolean isEmpty;
	};

	//	entry for hash table of frames
	private class bucket {
		public PageId page_number;
		public int frame_number;
	};

	private static byte[] bufPool;					// stores data of pages, as bytes
	private static descriptor[] bufDescr;		// descriptor of frames
	private static bucket[] directory;			// hash table of pageId and frame number

	private static float[] crf;							// crf value for LRFU policy
	private static float[][] ref;						// stores when pages are referenced

	private static int[] replaceCandidates;

	private static int numbufs;							// number of buffers
	private static int numPages;						// number of pages

	// variables for hash table
	private int htsize;
	private int a = 24;
	private int b = 50;

	// variables for LRFU policy
	private static int numrefs;
	private static float time;

	private static boolean f = false;

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

		this.numbufs = numbufs;
		this.htsize = minPrime;
		
		this.numrefs = 1000;
		replacementPolicy = "LRFU";

		bufPool = new byte[GlobalConst.PAGE_SIZE * numbufs];
		bufDescr = new descriptor[numbufs];
		directory = new bucket[htsize];
		//replaceCandidates = new int[numbufs];
		//crf = new float[numbufs];
		//ref = new float[numbufs][numrefs];

		for (int i=0; i<bufPool.length; i++) {
			bufPool[i] = 0;
		}

		for (int i=0; i<numbufs; i++) {
			bufDescr[i] = new descriptor();
			bufDescr[i].page_number = new PageId(-1);
			bufDescr[i].pin_count = 0;
			bufDescr[i].dirtybit = false;
			bufDescr[i].isEmpty = true;
		}

		for (int i=0; i<htsize; i++) {
			directory[i] = new bucket();
			directory[i].page_number = new PageId(-1);
			directory[i].frame_number = -1;
		}
	};


	/*
		pinPage.
	*/
	public void pinPage(PageId pageno, Page page, boolean emptyPage)
		throws ChainException, IOException, BufferPoolExceededException, InvalidPageNumberException {
if (f == true){
			// throw exception if all pages were already pinned
			boolean allPinned = true;
			for (int i=0; i<numbufs; i++) {
				if (bufDescr[i].pin_count == 0) {
					allPinned = false;
					break;
				}
			}
			if (allPinned == true) {
				throw new BufferPoolExceededException (null, "bufmgr.BufferPoolExceededException");
			}
			
			// if page in buffer pool, increment pin_count
			int hash_index = hash(pageno);
			directory[hash_index].page_number = pageno;
			int frame_index = directory[hash_index].frame_number;

			if (frame_index > -1) {

				// if pin_count was 0, remove from replaceCandidates
				if (bufDescr[frame_index].pin_count == 0) {
					for (int i=0; i<replaceCandidates.length; i++) {
						if (replaceCandidates[i] == pageno.pid) {
							replaceCandidates[i] = -1;
							break;
						}
					}
				}

				bufDescr[frame_index].pin_count++;
				
				// add reference time
				for (int i=0; i<numrefs; i++) {
					if (ref[frame_index][i] < 0) {
						ref[frame_index][i] = time;
						break;
					}
				}
			}
			else {
				// pick F from replaceCandidates
				// F = page (in frame) with min CRF value

				// calculate CRF for each frame

				for (int i=0; i<numPages; i++) {
					crf[i] = 0.0f;

					for (int j=0; j<numrefs; j++) {
						if (ref[i][j] > 0) {
							crf[i] += 1.0f/(time -ref[i][j] +1.0f);
						}
					}
				}

				float min = crf[0];
				int frame_index_replace = 0;

				for (int i=0; i<numPages; i++) {
					if (replaceCandidates[i] > -1) {
							if (crf[i] < min) {
								min = crf[i];
								frame_index_replace = i;
						}
					}
				}

//System.out.println(frame_index_replace + "=replacing frame");
				// replace F by P
				descriptor pageToReplace = bufDescr[frame_index_replace];

				// if this frame was holding a page
				if (pageToReplace.isEmpty == false) {
					int hash_index_replace = hash(pageToReplace.page_number);

					// if F.dirtybit = 1, flush page to disk
					if (pageToReplace.dirtybit == true) {
						flushPage(pageToReplace.page_number);
					}

					directory[hash_index_replace].page_number = new PageId(-1);
					directory[hash_index_replace].frame_number = -1;
				}

				directory[hash_index].page_number = pageno;
				directory[hash_index].frame_number = frame_index_replace;
				
				bufDescr[frame_index_replace].page_number = pageno;
				bufDescr[frame_index_replace].pin_count = 1;
				bufDescr[frame_index_replace].dirtybit = false;
				bufDescr[frame_index_replace].isEmpty = false;

				// read / write page content to buffer
				if (pageno == null) {
					throw new InvalidPageNumberException (null, "bufmgr.InvalidPageNumberException");
				}
				else {
					if (pageno.pid < 0) {
						throw new InvalidPageNumberException (null, "bufmgr.InvalidPageNumberException");
					}
				}
				Minibase.DiskManager.read_page(pageno, page);

				byte[] data = page.getData();

				for (int j=0; j<GlobalConst.PAGE_SIZE; j++) {
					bufPool[GlobalConst.PAGE_SIZE*frame_index_replace +j] = data[j];
				}

				// reset reference info
				for (int i=0; i<numrefs; i++) {
					ref[frame_index_replace][i] = -1;
				}

				// remove F from replaceCandidates
				for (int j=0; j<replaceCandidates.length; j++) {
					if (replaceCandidates[j] == frame_index_replace) {
						replaceCandidates[j] = -1;
					}
				}
			}
			
			time += 1.0f;
			}
		};

	/*
		unpinPage.
	*/
	public void unpinPage(PageId pageno, boolean dirty) 
		throws PageUnpinnedException {
if (f == true){
		// check if this page is in buffer pool
		int hash_index = hash(pageno);
		PageId page_id = directory[hash_index].page_number;
		int frame_index = directory[hash_index].frame_number;

		//	throw exception when try to unpin a page that is not pinned
		if (page_id == pageno && frame_index > -1) {
			if (bufDescr[frame_index].pin_count == 0) {
				throw new PageUnpinnedException (null, "bufmgr: PageUnpinnedException.");
			}

			// set dirty bit if page is already modified
			if (dirty == true) {
				bufDescr[frame_index].dirtybit = true;
			}

			bufDescr[frame_index].pin_count--;
//System.out.print((frame_index+1)+ "th pin: "+ bufDescr[frame_index].pin_count + ", ");
		
			// add page to replaceCandidates if decrementing pin_count results in 0
			if (bufDescr[frame_index].pin_count == 0) {

				boolean found = false;

				// find if this page already exists in replaceCandidates
				for (int i=0; i<replaceCandidates.length; i++) {
					if (replaceCandidates[i] == pageno.pid) {
						found = true;
						break;
					}
				}

				// if not, add to replaceCandidates
				if (found == false) {
					for (int i=0; i<replaceCandidates.length; i++) {
						if (replaceCandidates[i] < 0) {
							replaceCandidates[i] = pageno.pid;
							break;
						}
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
f = true;
		// initialize page info
		bufDescr = new descriptor[numbufs];
		directory = new bucket[htsize];
		time = 1.0f;

		for (int i=0; i<numbufs; i++) {
			bufDescr[i] = new descriptor();
			bufDescr[i].page_number = new PageId(-1);
			bufDescr[i].pin_count = 0;
			bufDescr[i].dirtybit = false;
			bufDescr[i].isEmpty = true;
		}
		for (int i=0; i<htsize; i++) {
			directory[i] = new bucket();
			directory[i].page_number = new PageId(-1);
			directory[i].frame_number = -1;
		}

		numPages = howmany;
		replaceCandidates = new int[howmany];
		crf = new float[howmany];
		ref = new float[howmany][numrefs];

		for (int i=0; i<howmany; i++) {
			crf[i] = -1.0f;
			for (int j=0; j<numrefs; j++) {
				ref[i][j] = -1;
			}
			replaceCandidates[i] = i;
		}

		PageId pageno = Minibase.DiskManager.allocate_page(howmany);

		// check if buffer pool is full
		boolean isNotFull = true;
		for (int i=0; i<bufDescr.length; i++) {
			if (bufDescr[i].isEmpty == false) {
				isNotFull = false;
				break;
			}
		}
		if (isNotFull = false) {

			Minibase.DiskManager.deallocate_page(pageno, howmany);
			return null;
		}

		// if not, find a frame in the buffer pool
		for (int i=0; i<bufDescr.length; i++) {
			if (bufDescr[i].isEmpty == true) {

				// update descriptor
				bufDescr[i].page_number = pageno;
				bufDescr[i].pin_count = 1;
				bufDescr[i].dirtybit = false;
				bufDescr[i].isEmpty = false;

				// update hash table
				int hash_index = hash(pageno);
				directory[hash_index].page_number = pageno;
				directory[hash_index].frame_number = i;

				// add page content to buffer
				byte[] data = firstpage.getData();

				for (int j=0; j<GlobalConst.PAGE_SIZE; j++) {
					bufPool[GlobalConst.PAGE_SIZE*i +j] = data[j];
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
		throws ChainException, PagePinnedException {
			
			if (globalPageId.pid > -1) {

				// throw exception when this page is already pinned
				int hash_index = hash(globalPageId);
				PageId page_id = directory[hash_index].page_number;
				int frame_index = directory[hash_index].frame_number;
				
				if (page_id == globalPageId && frame_index > -1) {
					if (bufDescr[frame_index].pin_count > 0) {
//System.out.print(frame_index+ "=frame ");
//System.out.print(bufDescr[frame_index].pin_count+ "=pin ");
//for (int i=0; i<numbufs; i++) System.out.print(bufDescr[i].pin_count + " ");
						throw new PagePinnedException(null, "bufmgr.PagePinnedException");
					}
				}

				Minibase.DiskManager.deallocate_page(globalPageId);
			}
	};

	/*
		flushPage.
	*/
	public void flushPage(PageId pageid) 
		throws ChainException, IOException, InvalidPageNumberException {

		if (pageid == null) {
			throw new InvalidPageNumberException (null, "bufmgr.InvalidPageNumberException");
		}
		else {
			if (pageid.pid < 0) {
				throw new InvalidPageNumberException (null, "bufmgr.InvalidPageNumberException");
			}
		}
		
		// find page in buffer pool
		int hash_index = hash(pageid);
		PageId page_id = directory[hash_index].page_number;
		int frame_index = directory[hash_index].frame_number;

		if (page_id == pageid && frame_index > -1) {

			// write content of page
			byte[] data = new byte[GlobalConst.PAGE_SIZE];

			for (int i=0; i<GlobalConst.PAGE_SIZE; i++) {
				data[i] = bufPool[GlobalConst.PAGE_SIZE*frame_index +i];
			}

			Page page = new Page(data);
			Minibase.DiskManager.write_page(pageid, page);
		}
	};

	/*
		flushAllPages.
	*/
	public void flushAllPages() 
		throws ChainException, IOException {
		for (int i=0; i<numbufs; i++) {
			if (bufDescr[i].isEmpty == false) {
				flushPage(bufDescr[i].page_number);
			}
		}
	};

	/*
		getNumBuffers.
	*/
	public int getNumBuffers() {
		return numbufs;
	}

	/*
		getNumUnpinned.
	*/
	public int getNumUnpinned() {
		int count = 0;
		for (int i=0; i<numbufs; i++) {
			if (bufDescr[i].pin_count==0) {count++;}
		}
		return count;
	}

	/*
		Hash function.
	*/
	public int hash(PageId pageno) {
		return (a*pageno.pid + b) % htsize;
	}

/*
	public PageId[] getR() {
		return r;
	}

	public void setR(PageId[] p) {
		r = p;
	}
*/
};