package heap;

import global.Convert;
import global.GlobalConst;
import global.Minibase;
import global.RID;
import global.Page;
import global.PageId;

import chainexception.ChainException;


public class HeapFile {

	// Hash table of <RID, Tuple> pairs.
	public class record {
		RID rid;
		Tuple tuple;

		public record() {
			rid = null;
			tuple = null;
		};
	}

	// Page directory class.
	public class directory {

		HFPage[] pageLocation;
		int[] free_space;

		public directory() {
			pageLocation = new HFPage[numPages];
			free_space = new int[numPages];

			for (int i=0; i<numPages; i++) {
				pageLocation[i] = null;
				free_space[i] = dummy.getFreeSpace();
			}
		};
	}


	int numRecord;
	int numPages;
	int numDir;

	directory[] dir;
	record[] records;
	int htsize;
	int a;
	int b;

	HFPage dummy = new HFPage();
	PageId header;
	Page headerPage;


	/*
		Constructor.
	*/
	public HeapFile(String name) {

		// try to open HeapFile
		PageId file = Minibase.DiskManager.get_file_entry(name);

		header = Minibase.DiskManager.allocate_page();
		headerPage = new Page();
		Minibase.DiskManager.read_page(header, headerPage);
		

		// no such HeapFile exists; create an empty file
		if (file == null) {
//System.out.println("file empty; create a new file");

			Minibase.DiskManager.add_file_entry(name, header);

//System.out.println("header pid: " + header.pid);

		//Minibase.DiskManager.write_page(headerPage, header);
		//Minibase.DiskManager.print_space_map();
		}

		// retrieve pages from given file
		else {

			// load header page
			PageId headerPid = new PageId();
			Page headerP = new Page();
//			Minibase.DiskManager.read_page(file, headerP);

			// load page directory from loaded header file

			// load hash table from loaded header file
		}


		// page directory
		numDir = 100;
		numPages = 100;
		dir = new directory[numDir];
		for (int i=0; i<numDir; i++) {
			dir[i] = new directory();
		}

		// hash table of <RID, Tuple>
		records = new record[getPrime(numDir*numPages*numPages)];
		for (int i=0; i<records.length; i++) {
			records[i] = new record();
		}

		numRecord = 0;
	};

	
	/*
		insert a record.
	*/
	public RID insertRecord(byte[] record)
		throws SpaceNotAvailableException {

		// throw exception if the size of record is too large
		if (record.length > GlobalConst.MAX_TUPSIZE) {
			throw new SpaceNotAvailableException("heap.SpaceNotAvailableException");
		}

		
		// first, look for free space in existing pages
		for (int i=0; i<numDir; i++) {
			if (dir[i] != null) {

				for (int j=0; j<numPages; j++) {
					if (dir[i].pageLocation[j] != null) {
						if (dir[i].free_space[j] > record.length) {

							//	insert record
							RID rid;
							rid = dir[i].pageLocation[j].insertRecord(record);
							numRecord++;

							// update free space
							dir[i].free_space[j] = dir[i].pageLocation[j].getFreeSpace();

							// create tuple
							Tuple tuple = new Tuple(record, 0, record.length);
							int hash_index = hash(rid);
							records[hash_index].rid = rid;
							records[hash_index].tuple = tuple;

							return rid;
						}
					}
				}
			}
		}

		// otherwise, allocate a new HFPage and insert record
		for (int i=0; i<numDir; i++) {
			if (dir[i] != null) {

				for (int j=0; j<numPages; j++) {
					if (dir[i].pageLocation[j] == null) {

						// allocate new HFPage
						HFPage hfp = new HFPage();
						dir[i].pageLocation[j] = hfp;
						PageId pid = new PageId(numDir*i + j);
						hfp.setCurPage(pid);

						// insert record
						RID rid;
						rid = hfp.insertRecord(record);
						numRecord++;

						// update free space
						dir[i].free_space[j] = hfp.getFreeSpace();

						// create tuple
						Tuple tuple = new Tuple(record, 0, record.length);
						int hash_index = hash(rid);

						records[hash_index].rid = rid;
						records[hash_index].tuple = tuple;
						return rid;
					}
				}
			}
		}

		return null;
	};


	/*
		return a record.
	*/
	public Tuple getRecord(RID rid) {
		int hashIndex = hash(rid);
		return records[hashIndex].tuple;
	};

	
	/*
		update a record.
	*/
	public boolean updateRecord(RID rid, Tuple newRecord)
		throws InvalidUpdateException {

		int pageno = rid.pageno.pid;
		int slotno = rid.slotno;

		int hashIndex = hash(rid);
		Tuple tuple = records[hashIndex].tuple;

		byte[] currData = tuple.getTupleByteArray();
		byte[] newData = newRecord.getTupleByteArray();

		// find the record
		int dirIndex = pageno / numDir;
		int pageIndex = pageno % numDir;
		HFPage hfp = dir[dirIndex].pageLocation[pageIndex];

		// throw exception if the length of new tuple is different from the original one
		if (newRecord.getLength() != tuple.getLength()) {
			throw new InvalidUpdateException ();
		}

		// update tuple
		tuple.setTupleByteArray(newData);

		return true;
	};


	/*
		delete a record.
	*/
	public boolean deleteRecord(RID rid) {

		int pageno = rid.pageno.pid;
		int slotno = rid.slotno;

		// find the record
		int dirIndex = pageno / numDir;
		int pageIndex = pageno % numDir;
		HFPage hfp = dir[dirIndex].pageLocation[pageIndex];

		// delete it
		hfp.deleteRecord(rid);
		numRecord--;

		// update free space
		dir[dirIndex].free_space[pageIndex] = hfp.getFreeSpace();

		// if no record left in this page, remove the page
		if (hfp.firstRecord() == null) {
			dir[dirIndex].pageLocation[pageIndex] = null;
			dir[dirIndex].free_space[pageIndex] = dummy.getFreeSpace();
		}

		return true;
	};


	/*
		return the number of records.
	*/
	public int getRecCnt() {
		return numRecord;
	};


	/*
		start scanning the heap file.
	*/
	public HeapScan openScan() {
		HeapScan hs = new HeapScan(this);
		return hs;
	};

	/*
		return hash value of this rid.
	*/
	public int hash(RID rid) {
		return rid.hashCode();
	}

	/*
		pick min prime number >= val.
	*/
	public int getPrime(int val) {
		int minPrime = 0;
		int ret = val;

		while(true) {
			boolean isPrime = true;

			for (int j=2; j<ret; j++) {
				if (ret % j == 0) {
					isPrime = false;
					break;
				}
			}

			if (isPrime) {
				return ret;
			}

			ret++;
		}
	}

	/*
		get header pageid.
	*/
	public PageId getHeader() {
		return header;
	}


	/*
		get header page.
	*/
	public Page getHeaderPage() {
		return headerPage;
	}

	/*
		get hash table of <RID, Tuple> pairs.
	*/
	public record[] getRecords() {
		return records;
	}

	/*
		get total number of page directories.
	*/
	public int getNumDir() {
		return numDir;
	}

	/*
		get total number of pages within a single directory.
	*/
	public int getNumPages() {
		return numPages;
	}
}
