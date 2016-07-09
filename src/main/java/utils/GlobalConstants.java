package utils;

public class GlobalConstants {
	public static final int numThreads = 1;  // change it to 4 etc
	public static final double accFactor = 0.01;
	public static final int thresholdFlushToLog = 1;
	public static String dataSetType;//

	//
	public static boolean isCharInRange(char ch, char min, char max){
		if(ch >= min && ch <= max){
			return true;
		}
		else{
			return false;
		}
	}

	public static void setDataSetType(String experiRunID){
		if(experiRunID.indexOf("TAXI") != -1){
			GlobalConstants.dataSetType = "TAXI";
		}
		else if(experiRunID.indexOf("SYS") != -1){
			GlobalConstants.dataSetType = "SYS";
		}
		else if(experiRunID.indexOf("PLUG") != -1){
			GlobalConstants.dataSetType = "PLUG";
		}
	}

	public static String getDataSetTypeFromRunID(String experiRunID){
		if(experiRunID.indexOf("TAXI") != -1){
			return "TAXI";
		}
		else if(experiRunID.indexOf("SYS") != -1){
			return "SYS";
		}
		else if(experiRunID.indexOf("PLUG") != -1){
			return "PLUG";
		}
		return null;
	}
	//
}
