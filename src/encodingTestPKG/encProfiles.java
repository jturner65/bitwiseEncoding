package encodingTestPKG;

import java.util.*;
import java.util.concurrent.*;

public class encProfiles implements Callable<encObj> {
	private Map<String, List<String>> tmpMap;
	private ProfileEncoder p;
	private codecStrAra encStr;
	private codecLongAra encLong;
	private codecIntAra encInt;
	private int sizeEncStr,  numCatsToEnc;
	private encObj encObj;

	public encProfiles(ProfileEncoder _p,TreeMap<String, List<String>> _tmpMap, int _sizeEncStr, int _numCatsToEnc) {
		p = _p;
		tmpMap = _tmpMap;
		sizeEncStr = _sizeEncStr;
		numCatsToEnc = _numCatsToEnc;
		encStr = new codecStrAra(sizeEncStr, numCatsToEnc);
		encLong = new codecLongAra(numCatsToEnc);
		encInt = new codecIntAra(numCatsToEnc);
		encObj = new encObj();
	}
	//results go in:
	//encode all visitor ID-keyed profile category lists from individual strings to arrays of encoded strings, longs and ints
	@Override
	public encObj call() throws Exception {
		//encode each 
		for(String vID : tmpMap.keySet()){ 		//for every visitor ID
			List<String> tmpCatMap = tmpMap.get(vID);
			for(String cat : tmpCatMap){		//for every vID category from profile
				catEncodeDat tmpCEnc = p.catEncodings.get(cat);
				//System.out.println("Cat enc code : " + tmpCEnc.toString());
				encStr.setCustEncVal(tmpCEnc.getStrEnc());
				encInt.setCustEncVal(tmpCEnc.getIntEnc());
				encLong.setCustEncVal(tmpCEnc.getLongEnc());				
			}	
			
			encObj.encProfCatData_Str.put(vID,encStr.getFinalVal());
			encObj.encProfCatData_Int.put(vID,encInt.getFinalVal());
			encObj.encProfCatData_Long.put(vID,encLong.getFinalVal());
			//reset for next visitor id
			encStr.reinit();
			encInt.reinit();
			encLong.reinit();
		}
		return encObj;
	}// call() 

}

class encObj {
	//result structure from encoding each profile's categories
	public ConcurrentSkipListMap<String, String[]> encProfCatData_Str;
	public ConcurrentSkipListMap<String, Integer[]> encProfCatData_Int;
	public ConcurrentSkipListMap<String, Long[]> encProfCatData_Long;
	
	public encObj(){
		encProfCatData_Str = new ConcurrentSkipListMap<String, String[]>();
		encProfCatData_Int = new  ConcurrentSkipListMap<String, Integer[]>();
		encProfCatData_Long = new  ConcurrentSkipListMap<String, Long[]>();
	}
	
	
	
}//class encObj