package encodingTestPKG;

import java.util.*;

//class holding all functionality to encode a set of categories based on some metric (defaults to category prevalence)
//to generate a list of encodings to then be used directly to update visitor profiles
public class CategoryEncoder {
	//ref to calling main, to access processing libraries
	public encodingMain p;							
	//read in all categories from csv or database (TODO), then make up counts for each category that follows the appropriate distribution
	public Map<catCount, Integer> catsAndCounts;
	public Map<String,catCount> catsToCatCounts;
	//all encoded values for each category, for each possible encoding
	public ArrayList<catEncodeDat> encodedCatsAra;	
	public String srcFileName,					//csv file name holding source of categories to be encoded.
				destFileName;					//csv file name to save encoding map of all categories to.	
	//////
	///encoding constants
	//////
	public int numCatsToEnc;		//max # of categories supported by this encoding - make this as small as possible while covering category terrain	
	public int sizeEncStr;						//each encoding string is sizeEncStr chars long

	private int[] stFlags;						//state flags - bits in array holding relevant process info
	public static final int
			debugIDX = 0,						//enable debug mode
			dispCatDistIDX = 1,					//display distribution of categories
			calcDistValsIDX = 2,				//calculate artificial distribution values following power-law decay (for test)
			distBuiltIDX = 3,					//artificial distribution values have been calculated
			mPlexEncIDX = 4,					//whether or not to multiplex results based on some metric (Default cat count) - this means spread bits among resultant encoding units, instead of lumping all high-count bits in the first units
			encCatsIDX = 5;						//whether encoding needs to be done
	public static final int numFlags = 6;
	
	//for debug and display of distributions
	private final int numRecs = 1000000;		//# of profile records to use for debug mode distribution building
	//list of sorted catCounts, for display - to show decaying distribution
	private ArrayList<catCount> distVals;
	//value used to derive pareto dist
	private double alpha = .75;
	
	//ctor to read in cat and counts data from a csv
	public CategoryEncoder(encodingMain _proc, int _sizeOfEncString, int _numCatsToEnc, String _srcFilename, String _destFilename) {
		p = _proc; sizeEncStr = _sizeOfEncString; numCatsToEnc =_numCatsToEnc;	srcFileName = p.baseFileDir +_srcFilename;destFileName = p.baseFileDir +_destFilename;
		init();
		loadCategoryData();
		if(!getFlag(distBuiltIDX)){buildDistVals(alpha);}
		p.saveStrings(destFileName, encodeCategories());p.pr("Cat encoding saved to " + destFileName);
	}
	
	//ctor to take prebuild category and count map and build encoding file from this
	public CategoryEncoder(encodingMain _proc, int _sizeOfEncString, int _numCatsToEnc, String _destFilename, TreeMap<String,catCount> _catsToCC) {
		p = _proc; sizeEncStr = _sizeOfEncString; numCatsToEnc =_numCatsToEnc;	srcFileName = "";destFileName = p.baseFileDir +_destFilename;
		init();
		catsToCatCounts = _catsToCC;
		for(String c : catsToCatCounts.keySet()){
			catCount tmp =catsToCatCounts.get(c);
			catsAndCounts.put(tmp,tmp.count);
		}
		if(!getFlag(distBuiltIDX)){buildDistVals(alpha);}
	}
	
	private void init(){
		initFlags();
		setFlag(dispCatDistIDX, true);
		setFlag(mPlexEncIDX, true);		//default to multiplexing 
		catsAndCounts = new TreeMap<catCount, Integer>(Collections.reverseOrder());//TODO enable modification of reverse order to some custom comparator/sort criteria which will determine encoding order
		catsToCatCounts = new TreeMap<String,catCount>();
	}
	//initialize statemachine flags
	private void initFlags(){stFlags = new int[1 + numFlags/32]; for(int i = 0; i<numFlags; ++i){setFlag(i,false);}}
	public void setFlag(int idx, boolean val){
		int flIDX = idx/32, mask = 1<<idx%32;
		stFlags[flIDX] = (val ?  stFlags[flIDX] | mask : stFlags[flIDX] & ~mask);
		switch (idx) {//special actions for each flag
			case debugIDX : {break;}
			case dispCatDistIDX : {break;}
			case calcDistValsIDX : {break;}
			case distBuiltIDX : {break;}
			case mPlexEncIDX : {break;}
			case encCatsIDX : {break;}
		}
	}//setFlag	
	//get flag value
	public boolean getFlag(int idx){int bitLoc = 1<<(idx%32);return (stFlags[idx/32] & bitLoc) == bitLoc;}
	
	//load categories and counts from csv, built from following query : 
	//SELECT category_id,  COUNT(*) AS catCount	FROM segment_size	GROUP BY category_id;
	//list of categeories without counts can also be loaded, then (meaningless)counts following power law decay will be generated randomly
	private void loadCategoryData(){//loads from csv
		String[] strs = p.loadFileIntoStringAra(srcFileName, "loaded cat file :"+srcFileName, "Error reading file "+srcFileName);
		if(strs==null){return;}
		for (int i=0;i<strs.length;++i){
			String[] tokens = strs[i].split(",");			
			int count = 0;
			try{
				count = Integer.parseInt(tokens[1]);
			} catch (Exception e){setFlag(calcDistValsIDX,true);}//no counts provided, generate them based on power-rule decaying distribution
			catCount tmp = new catCount(tokens[0], count);
			catsAndCounts.put(tmp, count);			
		}//for every string in file data	
		p.pr("Done reading all categories from cat file : "+srcFileName+"\tLength : " + strs.length+" into cat map size : " + catsAndCounts.size());
	}//buildSampleData
	
	//build structure holding category encoding and save to file
	public String[] encodeCategories(){
		catEncodeDat tmpCatEnc;
		encodedCatsAra = new ArrayList<catEncodeDat>();
		codecStrAra encStr = new codecStrAra(sizeEncStr, numCatsToEnc);
		codecLongAra encLong = new codecLongAra(numCatsToEnc);
		codecIntAra encInt = new codecIntAra(numCatsToEnc);
		int numStrToEncode = encStr.getSizeReq(), numLongEnc = encLong.getSizeReq(), numIntEnc = encInt.getSizeReq();
		ArrayList<String> encCatData = new ArrayList<String>();
		encCatData.add(numStrToEncode + " byte arrays required to encode " + numCatsToEnc + " Categories into arrays of size : "+sizeEncStr);
		encCatData.add(numLongEnc + " 64bit longs required to encode " + numCatsToEnc + " Categories in 64 bit values.");
		encCatData.add(numIntEnc + " 32bit integers required to encode " + numCatsToEnc + " Categories in 32 bit values.");
		encCatData.add("Multiplexed categories by count across encoding : "  + (getFlag(mPlexEncIDX) ? "True" : "False"));
		encCatData.add("category,counts in profile,cat rank,enc idx 32, bit pos 32, enc idx 64, bit pos 64, enc idx string ara, enc idx char ara, bit pos in char");
		int idx=0;
		if (getFlag(mPlexEncIDX)){//spread highly occuring categories among all values in encoding vector based on sorting criteria of catsAndCounts
			int bitPos32 = encInt.getStartBitPos(), bitPos64=encLong.getStartBitPos(), bitPosChar=7, charPos=encStr.getStartBitPos(),idx32 = 0, idx64 = 0, idxStr = 0;
			for(catCount key : catsAndCounts.keySet()){			
				tmpCatEnc = new catEncodeDat(key.cat, key.count);
				//setAllData(int _idx, int _idx32, int _idx64, int _idxStr, int _idxCh, int _enc32, int _enc64, int _encCh){
				tmpCatEnc.setAllData(idx,idx32,idx64,idxStr,charPos,bitPos32,bitPos64,bitPosChar);//returns bit position within character within string
				++idx; ++idx32; ++idx64; ++idxStr;
				if(idx32 % numIntEnc == 0){--bitPos32; idx32 = 0;}
				if(idx64 % numLongEnc == 0){--bitPos64; idx64 = 0;}
				if(idxStr % numStrToEncode == 0){idxStr = 0; if(bitPosChar == 0){--charPos;	bitPosChar=7;} else {--bitPosChar;}}
				encodedCatsAra.add(tmpCatEnc);
				encCatData.add(tmpCatEnc.toString());
			}			 	
		} else {//sequential encoding based on sorting criteria of catsAndCounts
			for(catCount key : catsAndCounts.keySet()){			
				tmpCatEnc = new catEncodeDat(key);
				tmpCatEnc.setAllData(idx,encInt.getAraIDX(idx),	encLong.getAraIDX(idx),encStr.getAraIDX(idx),encStr.getBitPos(idx),		//returns character within string 
								encInt.getBitPos(idx), encLong.getBitPos(idx),encStr.getCharAraIDX(idx));//returns bit position within character within string
				++idx;
				encodedCatsAra.add(tmpCatEnc);
				encCatData.add(tmpCatEnc.toString());
			}	
		}
		setFlag(encCatsIDX,false);
		return encCatData.toArray(new String[1]);
	}//encodeCategories
	
	//plot distribution of values to screen
	public void plotDistVals(){
		if(!getFlag(dispCatDistIDX)){return;}
		p.pushMatrix();
		float barHeight = .9f*p.height;
		p.translate(0,1.1f*barHeight);
		float xVal = 0, yVal = 0;
		int numVals = distVals.size();
		float st = .05f*p.width;
		float MaxVal =  1.0f * distVals.get(0).count;
		for(int i =0; i<numVals;++i){
			xVal = st + (i/(1.0f*numVals))*(.9f*p.width);
			yVal = (distVals.get(i).count/MaxVal) * (-barHeight);
			//p.pr("#vals :  " +distVals.size() + " xval : " + xVal+ "\tyval : "+yVal);
			p.line(xVal, 0, xVal, yVal);
		}	
		p.popMatrix();
	}	
	//alpha value determines pareto dist shape	
	//numRecs acts as normalizer (this is only for debug purposes - assigns random probs following decaying power law to each cat)
	private void setDistVals(double alpha){
		//double alpha = .9;
		double xMinPos = .0000001;
		Random defaultR = new Random();
		for(catCount key : catsAndCounts.keySet()){
			Integer numThisCat = (int)((getParetoVal(defaultR, alpha, xMinPos) * numRecs) + 1);
			key.count = numThisCat;
			catsAndCounts.put(key,numThisCat);
		}
	}//setDistVals

	//build sorted list of category counts, to display
	private void buildDistVals(double alpha){
		if(getFlag(calcDistValsIDX)){setDistVals(alpha);}			//if no counts already existed, randomly assign power-law decay probs to all cats
		Comparator<catCount> comparator = Collections.reverseOrder();
		distVals = new ArrayList<catCount>(catsAndCounts.keySet());
		Collections.sort(distVals, comparator);
		if(getFlag(debugIDX)){for(int i =0; i<distVals.size();++i){if(distVals.get(i).count > 1000000){if(i%10 == 0){p.pr("");}	System.out.print("element:"+i+"\t"+distVals.get(i)+"\t\t");}}p.pr("");}		
		setFlag(distBuiltIDX,true);
	}//buildDistVals
	
	//generate pareto distribution described by xMinPos and alpha val
	//investigate other powerlaw distributions
	private double getParetoVal(Random r, double alpha, double xMinPos){
		double v = r.nextDouble();
		while (v == 0){			v = r.nextDouble();		}		
		//return xMinPos / Math.pow(v, 1.0/alpha);
		return Math.pow(xMinPos / v, alpha);
	}//getParetoVal

}//class CategoryEncoder

//class that holds a category string and a count of this category's presence in data.  implements comparable that first sorts by count then by string precedence, so can use
//in sorted structures - storing sorts automatically.  use red-black constructs (i.e. treemap) for best performance
class catCount implements Comparable<catCount>{
	public String cat;
	public int count;
	public catCount (String _cat, int _count){		cat = _cat.trim(); count = _count;	}
	@Override
	//comparable criteria based primarily on count, then, if equal count, on string value (so no collisions except for exact same data, since cat string should always be unique) 
	//changing this will determine mechanism of sequential/multiplexed organization of encodings
	public int compareTo(catCount arg0) {	return (this.count > arg0.count ? 1 : this.count < arg0.count ? -1 : this.cat.compareTo(arg0.cat));}//only compare string value if counts are equal
	@Override
	public String toString(){return "Cat:"+cat+"\tCount:"+count;}
}//catCount

//class to hold encoded value for particular category 
class catEncodeDat implements Comparable<catEncodeDat> {
	public catCount catCnt;								//category and count of occurrences
	//char encoding is character within string(char ara), within string array(where each string is a column in the storage table)
	public int idx, idx32, idx64, idxStr, idxCh;		//idx for 32 bit, 64 bit, string and character (16 bit) encoding (i.e. which encoded value in array of values this cat resides in)
	//encodings are bit masks for this category based on particular encoding method - all unsigned
	public int bitPos32,bitPos64, bitPosChar;					//encoding in 32 bit position, 64 bit position,(16 bit) - bit position
	
	public catEncodeDat(catCount _catCnt){catCnt = _catCnt;}
	public catEncodeDat(String _catName, int _count){this(new catCount(_catName, _count));}
	public catEncodeDat(String[] encData){
		this(new catCount(encData[0],Integer.parseInt(encData[1])));
		setAllData(Integer.parseInt(encData[2]),Integer.parseInt(encData[3]),Integer.parseInt(encData[5]),
				Integer.parseInt(encData[7]),Integer.parseInt(encData[8]),Integer.parseInt(encData[4]),
				Integer.parseInt(encData[6]),Integer.parseInt(encData[9]) );
		//p.pr("EncString Ctor :" + this.toString());
	}
	
	public void setAllData(int _idx, int _idx32, int _idx64, int _idxStr, int _idxCh, int _enc32, int _enc64, int _encCh){
		idx = _idx; idx32  = _idx32 ;idx64  = _idx64 ;idxStr = _idxStr;idxCh  = _idxCh ;
		bitPos32  = _enc32;bitPos64  = _enc64;bitPosChar  = _encCh ;
	}
	//get encoding arrays used by encoder/decoder - idx 0 is bit position, 1 is index in array (2 is char index in string for string arrays)
	public int[] getIntEnc(){return new int[]{bitPos32, idx32};}
	public int[] getLongEnc(){return new int[]{bitPos64, idx64};}
	public int[] getStrEnc(){return new int[]{bitPosChar, idxCh, idxStr};}
	
	@Override
	public int compareTo(catEncodeDat o) {		return catCnt.compareTo(o.catCnt);	}
	@Override
	public String toString(){return catCnt.cat+", "+catCnt.count+", " + idx +", "+ idx32 +", "+ bitPos32 +", "+ idx64 +", "+ bitPos64 +", "+ idxStr +", "+ idxCh +", "+bitPosChar;}

}//class catEncodeDat

//class to hold decoder config for each type of encoding for each catcnt
class catDecodeDat implements Comparable<catDecodeDat> {
	public catCount catCnt;
	public int[] idxs;				//either 2 or 3 int idxs for ara and bitpos of possible encoding	
	public int type;				//0 : str, 1 : 32 int , 2 : 64 int (long)
	public catDecodeDat(catCount _cc, int _type, int[] _idxs){
		_cc = new catCount(_cc.cat, _cc.count);
		type = _type;
		idxs = new int[ _idxs.length];
		for(int i=0;i< _idxs.length;++i){idxs[i] = _idxs[i];}		
	}
	@Override
	public int compareTo(catDecodeDat arg0) {		
		for(int i=idxs.length-1;i>=0;--i){if(idxs[i] > arg0.idxs[i]){ return 1;}else if(idxs[i] < arg0.idxs[i]){return -1;}}//if equal try next lower idx
		//if makes it here then identical encodings - sort by catcnt obj - should return 0, since identical encodings should mean identical catCnt objs, so this is redundant
		return catCnt.compareTo(arg0.catCnt);
	}//comparison
	@Override
	public String toString(){
		String res =  catCnt.cat+", "+catCnt.count+", " + type +", "+ idxs.length +", ";
		for(int i=idxs.length-1;i>=0;--i){res += idxs[i]+", ";}
		return res;
	}
	
}//class catDecodeDat
