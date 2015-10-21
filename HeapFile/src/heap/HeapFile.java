package heap;

import global.Convert;
import global.GlobalConst;
import global.Minibase;
import global.RID;

import java.io.IOException;

import chainexception.ChainException;

public class HeapFile {

	String name;
	int numRecord;


	/*
		Constructor.
	*/
	public HeapFile(String name) {
		this.name = name;
		numRecord = 0;
	};

	
	/*
		insert a record.
	*/
	public RID insertRecord(byte[] record)
		throws SpaceNotAvailableException {
		numRecord++;

		return null;
	};


	/*
		return a record.
	*/
	public Tuple getRecord(RID rid) {
		return null;
	};

	
	/*
		update a record.
	*/
	public boolean updateRecord(RID rid, Tuple newRecord)
		throws InvalidUpdateException {
		
		return false;
	};


	/*
		delete a record.
	*/
	public boolean deleteRecord(RID rid) {
		numRecord--;

		return false;
	};


	/*
		return the number of records
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
}
