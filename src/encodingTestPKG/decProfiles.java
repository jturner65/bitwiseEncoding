package encodingTestPKG;

import java.util.*;
import java.util.concurrent.*;

public class decProfiles implements Callable<Boolean> {
	private Map<String, List<String>> tmpMap;
	private ProfileEncoder p;
	private codecStrAra encStr;
	private codecLongAra encLong;
	private codecIntAra encInt;
	private int sizeEncStr,  numCatsToEnc;

	public decProfiles(ProfileEncoder _p,TreeMap<String, List<String>> _tmpMap, int _sizeEncStr, int _numCatsToEnc) {
		p = _p;
		tmpMap = _tmpMap;
		sizeEncStr = _sizeEncStr;
		numCatsToEnc = _numCatsToEnc;
		encStr = new codecStrAra(sizeEncStr, numCatsToEnc);
		encLong = new codecLongAra(numCatsToEnc);
		encInt = new codecIntAra(numCatsToEnc);
	}
	//results go in:
	//encode all visitor ID-keyed profile category lists from individual strings to arrays of encoded strings, longs and ints
	@Override
	public Boolean call() throws Exception {
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
			
			p.encProfCatData_Str.put(vID,encStr.getFinalVal());
			p.encProfCatData_Int.put(vID,encInt.getFinalVal());
			p.encProfCatData_Long.put(vID,encLong.getFinalVal());
			//reset for next visitor id
			encStr.reinit();
			encInt.reinit();
			encLong.reinit();
		}
		return true;
	}// call() 

}
