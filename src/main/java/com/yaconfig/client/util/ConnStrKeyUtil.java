package com.yaconfig.client.util;

import com.yaconfig.client.Constants;

public class ConnStrKeyUtil {
	public static String getKeyNameFromStr(String str){
		String remoteKey = str.substring(str.indexOf(Constants.CONNECTION_KEY_SEPERATOR) + 1);
		return remoteKey;
	}
	
	public static String getConnStrFromStr(String str){
		String connStr = str.substring(0,str.indexOf(Constants.CONNECTION_KEY_SEPERATOR));
		return connStr;
	}
	
	public static String makeLocation(String connStr,String key){
		return connStr + Constants.CONNECTION_KEY_SEPERATOR + key;
	}
}
