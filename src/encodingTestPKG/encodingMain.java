package encodingTestPKG;

import processing.core.*;
import java.util.*;
import java.util.concurrent.*;

public class encodingMain extends PApplet{

	//public String catFileName ="bluekai_mapping.csv"; 
	//public String catCountSrcFileName ="segCats.csv"; 					//source file of categories, and their counts. (without counts, a power law dist will be built for them)
	public String catCountSrcFileName ="segCats_BK.csv"; 					//source file of categories, and their counts. (without counts, a power law dist will be built for them)
	//public String catEncSaveFileName = "EncodedCatVals.csv";			//output of encoding into character string arrays, int arrays and double arrays
	public String catEncSaveFileName = "EncodedCatVals_BK.csv";			//output of encoding into character string arrays, int arrays and double arrays
	public final String baseFileDir = "data/";
	//# of categories to encode - build encodings based on this value - keep as low as possible to minimize encoding size while still big enough to support all cats
	public final int numCatsToEnc = 16000;
	//each encoding string is sizeEncStr chars long
	public final int sizeEncStr	=	120;	
	//this encodes a set of categories and builds a mapping for various encoder processes, which then can be used to process visitor records directly
	public CategoryEncoder catEnc;
	public boolean buildCatCntMap = true;//build map in profile encoder from profile data
		
	///////
	//profile encoding
	///////
	public String vndr = "bluekai";
	
	public String f1ToCat = vndr + "_mapping.csv";							//mapping of vendor 3rd party cats to our cats
	public String fPrfSrc = vndr + "_data_admins.log";					//profiles : visitors with multi categories.  dupe visitors need to be handled
	public String fPrfDestBase = "Encoded_Prfls";								//base file name for profiles encoded with visitor id and category encoding
	
	//encode all categories in a profile
	public ProfileEncoder profEnc;
	//for multithreading 
	public ExecutorService th_exec;
	public int numThreadsAvail;
	
	private void init(){
		int start, end;
		//for(int i = 1; i< 48; ++i){//1 == 207377
			start = millis();				
			//CategoryEncoder(encodingMain _proc, int _sizeOfEncString, int _numCatsToEnc, String _srcFilename, String _destFilename)
			if(!buildCatCntMap){
				catEnc = new CategoryEncoder(this, sizeEncStr, numCatsToEnc, catCountSrcFileName, catEncSaveFileName);
			}
			numThreadsAvail =  1*Runtime.getRuntime().availableProcessors();		//TODO derive this based on machine.
			pr("#threads : "+numThreadsAvail);
			th_exec = Executors.newCachedThreadPool();
			//th_exec = Executors.newFixedThreadPool(numThreadsAvail);
			
			// ProfileEncoder(encodingMain _p, String _f1toCat, String _fEncCats, String _fPrfSrc, String _fPrfDestBase, boolean buildCatCntMap) 
			//profile encoding called here
			profEnc = new ProfileEncoder(this, f1ToCat, catEncSaveFileName, fPrfSrc, fPrfDestBase, buildCatCntMap);
			pr("Exec time for #threads : "+numThreadsAvail + " " + (millis()-start));
			th_exec.shutdown();
		//}
	}

	//processing stuff for visualization
	public void settings(){size(800,600, P2D);	}		
	public void setup(){//runs 1 time
		init();
		colorMode(RGB, 1.0f);
	}	
	public void draw(){//runs repeatedly, 
		background(0);
		stroke(1.0f,0,0);
		strokeWeight(1.0f);
		if(!buildCatCntMap){	catEnc.plotDistVals();} 
		else {			profEnc.plotDistVals();		}
		
	}//draw
	//scut to print to console
	public void pr(String str){System.out.println(str);}
	public void prt(String str){System.out.print(str);}
	public String[] loadFileIntoStringAra(String fileName, String dispYesStr, String dispNoStr){
		String[] strs = null;
		try{
			strs = loadStrings(fileName);
			System.out.println(dispYesStr+"\tLength : " + strs.length);
		} catch (Exception e){System.out.println(dispNoStr);return null;}
		return strs;		
	}//loadFileIntoStrings
	
	
	public static void main(String[] args) {
		PApplet.main(new String[] {  "encodingTestPKG.encodingMain"});
	}

}//bloomFilterMain
