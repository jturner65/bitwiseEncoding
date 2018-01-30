package encodingTestPKG;

import java.nio.charset.StandardCharsets;
//test
public abstract class baseCoDecObj {
	public int fElemSize;		//size of element in encoded array in bits/components
	protected int numBitElems;			//# of elements to be encoded

	protected int numAraElems;		//# of elements in encoded array - # totl elements/size per element in bits
	
	public baseCoDecObj(int _fElemSize,int _numBitElems) {
		fElemSize = _fElemSize;
		numBitElems = _numBitElems;		//means # of total categories being encoded
		numAraElems = (int) (Math.ceil(numBitElems/(1.0*fElemSize)));
	}
	
	public abstract void reinit();	
	
	//get index into encoded ara of element - dependent on the size of the elements held in the enc ara
	public int getAraIDX(int idx){return idx/fElemSize;}
	//puts early IDX's in highest bits
	public int getBitPos(int idx){return (fElemSize-1) - (idx%fElemSize);}	//for string array this gives index into byte array		

	//return size of encoding array required to hold all components
	public int getSizeReq(){return numAraElems;}
	
	public int getStartBitPos(){return (fElemSize-1);}//NOTE : not bit pos for string array, gives starting character.  hence overridden in string encoder.

	//sets value at idx in filter to be true	- idx is # of bit in entire construct
	public abstract void setEncVal(int idx);
	//get whether particular array value is true
	public abstract boolean getEncVal(int idx);

	//sets values at indices to be used to encode category - these indecies are prebuilt for custom arrangement and loaded from a file or a db
	//for char, long or int encoding, there are 2 - the bit location (idx 0) and the ara component idx (idx 1)
	//for string encoding, there are 3 - the bit location (idx 0), the char ara component idx (idx 1), and the string ara component idx (idx 2)
	public abstract void setCustEncVal(int[] idxs);
	//get whether particular array value is true
	public abstract boolean getCustEncVal(int[] idxs);

}//encIntoAra

//into a single string (treat as char ara)
class codecByteAra extends baseCoDecObj{
	private byte[] encAra;	
	public codecByteAra(int _numBitElems) {
		super(8,_numBitElems);
		//make filter ara's length 1/32 of filterSize, to set and get values via div/mod of int values to minimize memory space
		encAra = new byte[numAraElems];
		reinit();
	}
	public void reinit(){for(int i =0; i<numAraElems;++i){	encAra[i]=0;}}//init with nulls
	
	//set initial value of character array from passed pre-encoded utf string
	public void setInitValue(String _str){
		byte[] tmpVals = _str.getBytes(StandardCharsets.UTF_8);
		int numElems = tmpVals.length;
		if(numElems > numAraElems){	numAraElems = numElems;	encAra = new byte[numAraElems];}
		System.arraycopy(tmpVals, 0, encAra, 0, numElems);
		//if less than elems, pad with 0
		for(int i =numElems; i<numAraElems;++i){	encAra[i]=0;}	//init with nulls
	}
	
	//sets values at indices to be used to encode category - these indecies are prebuilt for custom arrangement and loaded from a file or a db
	//for char, long or int encoding, there are 2 - the bit location (idx 0) and the ara component idx (idx 1)
	//for string encoding, there are 3 - the bit location (idx 0), the char ara component idx (idx 1), and the string ara component idx (idx 2)
	@Override
	public void setCustEncVal(int[] idxs) {	encAra[idxs[1]] |= (1<<idxs[0]);}
	@Override
	public boolean getCustEncVal(int[] idxs) {int bitIdx = 1<<idxs[0]; return ((encAra[idxs[1]] & bitIdx) == bitIdx);}	
	//sets value at idx in filter to be true
	@Override 
	public void setEncVal(int idx){	encAra[getAraIDX(idx)] |= 1<<getBitPos(idx);}
	//get whether particular array value is true
	@Override
	public boolean getEncVal(int idx){ int bitIdx = 1<<getBitPos(idx); return ((encAra[getAraIDX(idx)] & bitIdx) == bitIdx);}	
	
	public String getFinalStrVal(){return new String(encAra, StandardCharsets.UTF_8);}

}//class encIntoCharAra


//for string array treat like array of encCharAra (character arrays) so encoding/lookup only done 1 time
class codecStrAra extends baseCoDecObj{
	private codecByteAra[] encAra;			//each element is a character array of encoded data
	public codecStrAra(int _stringSize, int _numBitElems) {
		//string size is # chars per string == individual "element" size in encAra
		super(_stringSize*8,_numBitElems);
		encAra = new codecByteAra[numAraElems];
		for(int i =0; i<numAraElems;++i){	encAra[i]=new codecByteAra(fElemSize);	}
	}
	
	public void reinit(){for(int i =0; i<numAraElems;++i){	encAra[i].reinit();	}}//init with nulls
	@Override
	public int getStartBitPos(){
		return encAra[0].numAraElems -1;}//character starts at end of array
	
	//set all string array values
	public void setInitValues(String[] _strAra){	
		int lenStrAra = _strAra.length;
		if(lenStrAra > numAraElems){	numAraElems = lenStrAra;	encAra = new codecByteAra[numAraElems];}
		for(int i=0; i<lenStrAra; ++i){	encAra[i].setInitValue(_strAra[i]);}
		for(int i=lenStrAra; i<numAraElems;++i){	encAra[i]=new codecByteAra(fElemSize);	} //should be taken care of already
	}//setValues
	//sets values at indices to be used to encode category - these indecies are prebuilt for custom arrangement and loaded from a file or a db
	//for char, long or int encoding, there are 2 - the bit location (idx 0) and the ara component idx (idx 1)
	//for string encoding, there are 3 - the bit location (idx 0), the char ara component idx (idx 1), and the string ara component idx (idx 2)
	@Override
	public void setCustEncVal(int[] idxs) {	encAra[idxs[2]].setCustEncVal(idxs);}
	@Override
	public boolean getCustEncVal(int[] idxs) {return encAra[idxs[2]].getCustEncVal(idxs);}
	@Override //
	public void setEncVal(int idx) {	encAra[getAraIDX(idx)].setEncVal(getBitPos(idx));}
	@Override
	public boolean getEncVal(int idx) {return encAra[getAraIDX(idx)].getEncVal(getBitPos(idx));}
	//returns bit position within character in character array
	public int getCharAraIDX(int idx){return encAra[getAraIDX(idx)].getBitPos(getBitPos(idx));}
	
	public String[] getFinalVal(){
		String[] res = new String[encAra.length];
		for(int i=0;i<encAra.length;++i){res[i]=encAra[i].getFinalStrVal();}
		return res;
	}


}//class encIntoStringAra

class codecLongAra extends baseCoDecObj{
	private Long[] encAra;
	public codecLongAra(int _numBitElems) {
		super(Long.BYTES*8, _numBitElems);
		reinit();
	}
	public void reinit(){	encAra = new Long[numAraElems];for(int i =0; i<numAraElems;++i){	encAra[i]=0l;	}}
	
	//set all long array values
	public void setInitValues(Long[] _valAra){	
		int lenValAra = _valAra.length;
		if(lenValAra > numAraElems){	numAraElems = lenValAra;	encAra = new Long[numAraElems];}
		System.arraycopy(_valAra, 0, encAra, 0, lenValAra);
		//for(int i=lenStrAra; i<numAraElems;++i){	encAra[i]=0;	} //should be taken care of already
	}//setValues
	//sets values at indices to be used to encode category - these indecies are prebuilt for custom arrangement and loaded from a file or a db
	//for char, long or int encoding, there are 2 - the bit location (idx 0) and the ara component idx (idx 1)
	//for string encoding, there are 3 - the bit location (idx 0), the char ara component idx (idx 1), and the string ara component idx (idx 2)
	@Override
	public void setCustEncVal(int[] idxs) {	encAra[idxs[1]] |= 1<<idxs[0];}

	@Override
	public boolean getCustEncVal(int[] idxs) {int bitIdx = 1<<idxs[0]; return ((encAra[idxs[1]] & bitIdx) == bitIdx);}
	//sets value at idx in filter to be true
	@Override
	public void setEncVal(int idx){	encAra[getAraIDX(idx)] |= 1<<getBitPos(idx);}
	//get whether particular array value is true
	@Override
	public boolean getEncVal(int idx){ int bitIdx =  1<<getBitPos(idx); return ((encAra[getAraIDX(idx)] & bitIdx) == bitIdx);}	
	
	public Long[] getFinalVal(){	return encAra;}

}//class encIntoLongAra

class codecIntAra extends baseCoDecObj{
	private Integer[] encAra;
	public codecIntAra(int _numBitElems) {
		super(Integer.BYTES*8, _numBitElems);
		reinit();
	}
	public void reinit(){encAra = new Integer[numAraElems];for(int i =0; i<numAraElems;++i){	encAra[i]=0;	}}
	//set all long array values
	public void setInitValues(Integer[] _valAra){	
		int lenValAra = _valAra.length;
		if(lenValAra > numAraElems){	numAraElems = lenValAra;	encAra = new Integer[numAraElems];}
		System.arraycopy(_valAra, 0, encAra, 0, lenValAra);
		//for(int i=lenStrAra; i<numAraElems;++i){	encAra[i]=0;	} //should be taken care of already
	}//setValues
	
	//sets values at indices to be used to encode category - these indecies are prebuilt for custom arrangement and loaded from a file or a db
	//for char, long or int encoding, there are 2 - the bit location (idx 0) and the ara component idx (idx 1)
	//for string encoding, there are 3 - the bit location (idx 0), the char ara component idx (idx 1), and the string ara component idx (idx 2)
	@Override
	public void setCustEncVal(int[] idxs) {	encAra[idxs[1]] |= 1<<idxs[0];}

	@Override
	public boolean getCustEncVal(int[] idxs) {int bitIdx = 1<<idxs[0]; return ((encAra[idxs[1]] & bitIdx) == bitIdx);}

	//sets value at idx in filter to be true
	@Override
	public void setEncVal(int idx){	encAra[getAraIDX(idx)] |= 1<<getBitPos(idx);}
	//get whether particular array value is true
	@Override
	public boolean getEncVal(int idx){ int bitIdx =  1<<getBitPos(idx); return ((encAra[getAraIDX(idx)] & bitIdx) == bitIdx);}		
	public Integer[] getFinalVal(){	return encAra;}

}//class encIntoLongAra
