package encodingTestPKG;

import java.util.*;
import java.util.concurrent.*;

//this will encode profile information from category listings to either string ara, int ara, or long ara encodings
public class ProfileEncoder {
	//ref to calling main, to access processing libraries
	public encodingMain p;	
	//various file names to use for encoding or for source of data
	public String fileMapToIOneCats, fileMapOfCatEncs, fileProfileSrc, 
		fileEncPrfDest_Str,fileEncPrfDest_Int,fileEncPrfDest_Long ;		//string, int and long encoding dest files
	
	//cat and counts data built off profile data read in and categories found in ione mapping
	public TreeMap<String,catCount> prfCatsAndCounts;
	//all encoded values for each category, for each possible encoding
	public ArrayList<catEncodeDat> encodedCatsAra;	
	
	//result structure from encoding each profile's categories
	public ConcurrentSkipListMap<String, String[]> encProfCatData_Str;
	public ConcurrentSkipListMap<String, Integer[]> encProfCatData_Int;
	public ConcurrentSkipListMap<String, Long[]> encProfCatData_Long;
	//encoder to encde categories from loaded profile info
	public CategoryEncoder catEnc;

	//ordered map that holds all encodings for all categories keyed by category name
	public Map<String, catEncodeDat> catEncodings; 	
	//category mappings, from Ione to vndr, from vndr to Ione
	public Map<String, String> mapCatsToUs, mapCatsFromUs;
	//profile mappings from file - keyed by vis ID, value is array of cats connected to this vis id
	public List<TreeMap<String, List<String>>> profileMap;

	private int[] stFlags;						//state flags - bits in array holding relevant process info
	public static final int
			debugIDX = 0,						//enable debug mode
			buildCatCntMapIDX = 1;				//whether or not to build catAndCount-based encoder from loaded profile data
	public static final int numFlags = 2;
	public int numCatsToEncode;
	
	//multithread stuff
	public List<Future<encObj>> callEncProfFtrs;
	public List<encProfiles> callEncProfs;
	
	public int frameSize;	//how many visitor ids to send to each thread
	
	public ProfileEncoder(encodingMain _p, String _f1toCat, String _fEncCats, String _fPrfSrc, String _fPrfDestBase, boolean buildCatCntMap) {
		p=_p;
		fileMapToIOneCats = p.baseFileDir + _f1toCat;
		fileMapOfCatEncs = p.baseFileDir + _fEncCats;
		fileProfileSrc = p.baseFileDir + _fPrfSrc; 
		//destination files
		fileEncPrfDest_Str = p.baseFileDir + _fPrfDestBase + "_Str.csv";
		fileEncPrfDest_Int = p.baseFileDir + _fPrfDestBase + "_Int.csv";
		fileEncPrfDest_Long = p.baseFileDir + _fPrfDestBase + "_Long.csv";
		initFlags();
		setFlag(buildCatCntMapIDX, buildCatCntMap);
		//load all data and set up maps of profiles and resultant encodings		
		loadMappings();
		prfCatsAndCounts = new TreeMap<String, catCount>(Collections.reverseOrder());
		if(getFlag(buildCatCntMapIDX)){//build category encoding based on category data from profiles, instead of separate file
			loadProfiles();
		
			catEnc = new CategoryEncoder(p, p.sizeEncStr, numCatsToEncode, "cust_"+p.catEncSaveFileName, prfCatsAndCounts); 
			p.saveStrings(catEnc.destFileName, catEnc.encodeCategories());p.pr("Profile Cat encoding saved to " + catEnc.destFileName);
			//get encoding results
			encodedCatsAra = catEnc.encodedCatsAra;
			catEncodings = new TreeMap<String, catEncodeDat>();		
			for(catEncodeDat cat : encodedCatsAra){catEncodings.put(cat.catCnt.cat, cat);}			//meow bitches
		} 
		else {loadEncodings();	loadProfiles();}//build category encoding based on pre-calculated encodings in separate file or table
		//set up multi-threading constructs for encoding profile
		callEncProfFtrs = new ArrayList<Future<encObj>>();
		callEncProfs = new ArrayList<encProfiles>();	
		encodeProfiles();
	}//ProfileEncoder
	
	//initialize statemachine flags
	private void initFlags(){stFlags = new int[1 + numFlags/32]; for(int i = 0; i<numFlags; ++i){setFlag(i,false);}}
	public void setFlag(int idx, boolean val){
		int flIDX = idx/32, mask = 1<<idx%32;
		stFlags[flIDX] = (val ?  stFlags[flIDX] | mask : stFlags[flIDX] & ~mask);
		switch (idx) {//special actions for each flag
			case debugIDX : {break;}
			case buildCatCntMapIDX : {break;}
		}
	}//setFlag	
	//get flag value
	public boolean getFlag(int idx){int bitLoc = 1<<(idx%32);return (stFlags[idx/32] & bitLoc) == bitLoc;}
	
	
	//this will load mappings of their categories to our categories, and vice versa, and also the encoding mapping for each category
	private void loadEncodings(){
		catEncodings = new TreeMap<String, catEncodeDat>();		
		String[] strs = p.loadFileIntoStringAra(fileMapOfCatEncs, "loaded cat encoding file :"+fileMapOfCatEncs, "Error reading file "+fileMapOfCatEncs);
		if(strs==null){return;}
		catEncodeDat tmpEncDat;
		for (int i=0;i<strs.length;++i){
			String[] tkns = strs[i].split("\\s*,\\s*");
			if((tkns.length < 10) || tkns[0].toLowerCase().contains("cat")) {continue;}
			tmpEncDat = new catEncodeDat(tkns);
			catEncodings.put(tmpEncDat.catCnt.cat.trim(), tmpEncDat);
		}//for every string in file data	
		p.pr("Done reading all category encoding mappings from file : "+fileMapOfCatEncs+"\tLength : " + strs.length+" into cat encoding map size : " + catEncodings.size());
	}//loadEncodings
	
	private void setNumCatsFrameSize(int numEncodings){numCatsToEncode =  (int)(1.05*numEncodings);frameSize = 1 + numCatsToEncode/p.numThreadsAvail;}
	
	public void plotDistVals(){		catEnc.plotDistVals();	}
	
	//load in mappings and profiles
	private void loadMappings(){
		//get mappings from vndr categories to our categories, build maps going both ways
		mapCatsToUs = new TreeMap<String, String>();
		mapCatsFromUs = new TreeMap<String, String>();
		String[] strs = p.loadFileIntoStringAra(fileMapToIOneCats, "loaded cat-to-our-cat mapping file :"+fileMapToIOneCats, "Error reading file "+fileMapToIOneCats);
		if(strs==null){return;}

		//first idx in csv file is column headers
		String[] tkns = strs[0].split("\\s*,\\s*");
		String ours,theirs;
		int iOneIDX = (tkns[0].toLowerCase().contains("ione") ? 0 : 1),
			theirIDX = (iOneIDX+1)%2;
		for (int i=1;i<strs.length;++i){
			tkns = strs[i].split(",");
			ours = tkns[iOneIDX].trim();
			theirs = tkns[theirIDX].trim();
			mapCatsToUs.put(theirs, ours);
			mapCatsFromUs.put(ours, theirs);
		}//for every string in file data	
		p.pr("Done reading all cat-to-our-cat mappings from file : "+fileMapToIOneCats+"\tFile Length : " + strs.length+" into cat encoding map size : " + mapCatsFromUs.size() + " | ione column in file : " +iOneIDX);
		//for(String s : mapCatsToUs.keySet()){p.pr("their cat : "+ s + " -> our cat : " + mapCatsToUs.get(s));}
	}//loadMappings
	
	//multi functions to get rid of an if check - for the speedz
	//load profiles and build category/counts encoding map structure from profile data
	private void loadProfiles(){
		//global treemap - put all data in this map to start
		TreeMap<String, List<String>> tmpProfTreeMap = new TreeMap<String, List<String>>();
		String[] strs = p.loadFileIntoStringAra(fileProfileSrc, "loaded profile to cat mapping file :"+fileProfileSrc, "Error reading file "+fileProfileSrc);
		if(strs==null){return;}
		List<String> items;
		String prevVid = strs[0].split("\\t")[0].trim(), commaTkns;//previous visitor id
		TreeSet<String> mappedCats = new TreeSet<String>(), mappedOurCats = new TreeSet<String>(), 
				unMappedCats = new TreeSet<String>();
		for (int i=0;i<strs.length;++i){
			String[] baseTkns = strs[i].split("\\t");	String key = baseTkns[0].trim();//this is vis ID
			if(baseTkns.length == 1){			//format error in profile to cat mapping file - probably truncation of visitor id in previous record
				String[] prevAra = strs[i-1].split("\\t");
				//p.pr("baseTkns.length is "+baseTkns.length+" 1st str : " + baseTkns[0] + " and previous string : " + prevAra[prevAra.length-1] ); // append to previous vID				
				commaTkns = key;
				key = prevVid;
			} else {
				commaTkns = baseTkns[1].trim();		
			}
			String[] tkns = commaTkns.split("\\s*,\\s*");		
			//convert to ione cats
			items = convToIOneCatsAndCatCnts(tkns, mappedCats,mappedOurCats, unMappedCats);	
			if(items.size()==0){continue;}		//means none of the vndr categories mapped to anything in the ione mapping.  which is pretty bad.		
			List<String> tmpAra = tmpProfTreeMap.get(key);	//see if this vis id is already in map
			if(tmpAra == null){							tmpAra = items;}//not in map
			else if (tmpAra.size()>items.size()) {tmpAra.addAll(items);} 
			else {									items.addAll(tmpAra);  tmpAra = items;}
			tmpProfTreeMap.put(key,tmpAra);		
			prevVid = key;
		}//for every string in file data
		//for(String vid : tmpProfTreeMap.keySet() ){	p.prt("Vid : " + vid + " : Categories : " );for(String cat : profileMap.get(vid)){p.prt(" "+cat+",");}p.pr("");}

		showProfLoadDebug(strs.length,tmpProfTreeMap.size(),mappedCats.size(),unMappedCats.size());		
		setNumCatsFrameSize(mappedCats.size());
		buildProfileMap(tmpProfTreeMap);
		//showEncMapMatchDebug(mappedCats,mappedOurCats);
	}//loadCatCntProfiles
	//convert and build catCount structure from profile data to build profile-driven encoding
	private List<String> convToIOneCatsAndCatCnts(String[] raw, TreeSet<String> mappedCats,TreeSet<String> mappedOurCats, TreeSet<String> unMappedCats){
		List<String> res = new ArrayList<String>();		
		for(int i =0; i<raw.length; ++i){
			String vCat = raw[i].trim(),ioneCat = mapCatsToUs.get(vCat);
			if(ioneCat != null){
				String iCat = ioneCat.trim();
				res.add(iCat); mappedCats.add(vCat);mappedOurCats.add(iCat);
				
				catCount tmp = prfCatsAndCounts.get(iCat);
				if(tmp == null){	tmp = new catCount(iCat, 1);} 
				else {					tmp.count++;	}			
				prfCatsAndCounts.put(iCat, tmp);			
			}
			else {unMappedCats.add(vCat);}
		}
		return res;
	}//convToIOneCatsAndCatCnts
	
	private void buildProfileMap(TreeMap<String, List<String>> tmpProfTreeMap){
		TreeMap<String, List<String>> tmpTreeMap ;
		//now multiplex profiles into lists of treemaps to facilitate multithreading
		profileMap = Collections.synchronizedList(new ArrayList<TreeMap<String, List<String>>>() );
		int idxInTreeAra = 0, maxIdxTree = p.numThreadsAvail;
		List<String> items;
		for(String key : tmpProfTreeMap.keySet()){
			//one of maxIdxTree different treemaps, split up for multithreading
			int treeMapIdx = (idxInTreeAra % maxIdxTree);		//cycling to pre-build partitioned arrays for multi-threading
			if(profileMap.size() > treeMapIdx){	tmpTreeMap = profileMap.get(treeMapIdx);} //get reference from profile map
			else{	tmpTreeMap = new TreeMap<String, List<String>>();profileMap.add(treeMapIdx, tmpTreeMap);}	//make new treemap, put it in profile map
			idxInTreeAra++;
			//get listfrom 
			items = tmpProfTreeMap.get(key);	
			List<String> tmpAra = tmpTreeMap.get(key);	//see if this vis id is already in map
			if(tmpAra == null){							tmpAra = items;}//not in map
			else if (tmpAra.size()>items.size()) {tmpAra.addAll(items);} 
			else {									items.addAll(tmpAra);  tmpAra = items;}
			tmpTreeMap.put(key,tmpAra);
		}
		p.pr("List of treemaps of size : " +  profileMap.size() + " built for multithreading with size per thread : " + profileMap.get(0).size());		
	}//buildProfileMap
	
	public void encodeProfiles(){		
		//keyed by visitor id - should never collide since profile map is parsed by unique visitor ids
		encProfCatData_Str = new ConcurrentSkipListMap<String, String[]>();
		encProfCatData_Int = new  ConcurrentSkipListMap<String, Integer[]>();
		encProfCatData_Long = new  ConcurrentSkipListMap<String, Long[]>();
		p.pr("Launch "+ profileMap.size() + " threads to process encoding.");
		int start = p.millis();	
		for(TreeMap<String, List<String>> item : profileMap){	//should be # of threads
			callEncProfs.add(new encProfiles(this, item, p.sizeEncStr, this.numCatsToEncode));
		}
		try{
			callEncProfFtrs = p.th_exec.invokeAll(callEncProfs);
			for(Future<encObj> f : callEncProfFtrs){
				encObj tmp = f.get();
				encProfCatData_Str.putAll(tmp.encProfCatData_Str);
				encProfCatData_Int.putAll(tmp.encProfCatData_Int);
				encProfCatData_Long.putAll(tmp.encProfCatData_Long);				
			}
		}catch(Exception e){e.printStackTrace();}
		//p.th_exec.shutdown();

		p.pr("Exec time for #threads : "+p.numThreadsAvail + " " + (p.millis()-start));
		saveEncodedData(fileEncPrfDest_Str,0);
		saveEncodedData(fileEncPrfDest_Int,1);
		saveEncodedData(fileEncPrfDest_Long,2);
		
		p.pr("Done encoding all profiles");
	}//encodeProfiles
	//save result of encoding
	public void saveEncodedData(String fileName, int type){
		String[] vals = new String[encProfCatData_Str.size()];
		int idx = 0;
		switch (type){
			case 0 : {for(String s : encProfCatData_Str.keySet()){vals[idx]=s+",";String[] tmpCatMap = encProfCatData_Str.get(s);for(String encCat : tmpCatMap){vals[idx]+=encCat+",";}++idx;}break;}//string
			case 1 : {for(String s : encProfCatData_Int.keySet()){vals[idx]=s+",";Integer[] tmpCatMap = encProfCatData_Int.get(s);for(Integer encCat : tmpCatMap){vals[idx]+= encCat+",";}++idx;}break;}//int
			case 2 : {for(String s : encProfCatData_Long.keySet()){vals[idx]=s+",";Long[] tmpCatMap = encProfCatData_Long.get(s);for(Long encCat : tmpCatMap){vals[idx]+=encCat+",";}++idx;}break;}		//long
		}//switch
		p.saveStrings(fileName,vals);		
	}//saveEncodedData
	
	private void showProfLoadDebug(int strsLen, int profMapLen, int mapCatLen, int unmapCatLen){
		p.pr("Clearing all category-to-our-category mappings to free memory");
		mapCatsToUs = new TreeMap<String, String>();
		mapCatsFromUs = new TreeMap<String, String>();
		p.pr("Done reading all profile/cat mappings (conv to our cats) from file : "+fileProfileSrc+"\tLength : " +strsLen+" into temp profile map size : " +profMapLen);
		p.pr("Mapped Categories : "+mapCatLen +"\tUnmapped/not found categories : " + unmapCatLen);
	}
	
	private void showEncMapMatchDebug(TreeSet<String> mappedCats,TreeSet<String> mappedOurCats ){
		for(String s : catEncodings.keySet()){p.pr("cat enc : "+ s );}
		//check for presence in cat encodings
		TreeSet<String> unEncodedCats = new TreeSet<String>();	
		for(String cat : mappedOurCats){if((catEncodings.get(cat) == null) && (!unEncodedCats.contains(cat))){p.pr("Cat not found in mapped : " + cat);unEncodedCats.add(cat);}}
		p.pr("# of cats in data that haven't been encoded : "  + unEncodedCats.size()+ " out of :"+ mappedCats.size());
	}
	

}//class ProfileEncoder
