package heap;

import global.Convert;
import global.GlobalConst;
import global.Minibase;
import global.RID;

import java.io.IOException;

import chainexception.ChainException;


/*
	Tuple class.
*/
public class Tuple {

	byte[] data;
	int start_index;
	int length;


	/*
		Empty constructor.
	*/
	public Tuple() {};


	/*
		Constructor.
	*/
	public Tuple(byte[] data, int start_index, int length) {
		this.data = data;
		this.start_index = start_index;
		this.length = length;
	};


	/*
		return length of the tuple.
	*/
	public int getLength() {
		return length;
	};


	/*
		return the data of this tuple.
	*/
	public byte[] getTupleByteArray() {
		return data;
	}
}
