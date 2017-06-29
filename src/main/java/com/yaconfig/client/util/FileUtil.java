package com.yaconfig.client.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import com.google.common.io.Files;

public class FileUtil {
	
	public static synchronized void writeValueToFile(String location,String value) {
		String path = ConnStrKeyUtil.getConnStrFromStr(location);
		String key = ConnStrKeyUtil.getKeyNameFromStr(location);
		File thefile = Paths.get(path).toAbsolutePath().toFile();
		OutputStreamWriter writer = null;

		try {
			if(thefile.exists()){
				File tmpfile = new File(path + ".yatmp" + System.nanoTime());
				writer = new OutputStreamWriter(new FileOutputStream(tmpfile));
				boolean isModify = false;
				List<String> lines = Files.readLines(thefile, Charset.forName("UTF-8"));
				for(String line : lines){
					if(line.contains("=")){
						String name = line.substring(0,line.lastIndexOf('='));
						if(name.equalsIgnoreCase(key)){
							writer.write(key + "=" + value + "\r");
							isModify = true;
						}else{
							writer.write(line + "\r");
						}
					}else{
						writer.write(line + "\r");
					}
				}
				
				if(!isModify){
					writer.write(key + "=" + value + "\r");
					isModify = true;
				}
				
				writer.flush();
				writer.close();
				
				if(thefile.delete()){
					Files.copy(new File(tmpfile.getAbsolutePath()), new File(thefile.getAbsolutePath()));
					tmpfile.delete();
				}
			}else{
				thefile.createNewFile();
				writer = new OutputStreamWriter(new FileOutputStream(thefile));
				writer.write(key + "=" + value);
				writer.flush();
			}
		}catch (IOException e) {
			e.printStackTrace();
		}finally{
			if(writer != null){
				try {
					writer.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}
	
	public static synchronized String readValueFromFile(String key) {
		String path = ConnStrKeyUtil.getConnStrFromStr(key);
		String fieldName = ConnStrKeyUtil.getKeyNameFromStr(key);
		File file = Paths.get(path).toAbsolutePath().toFile();

		if(file.isFile() && file.exists()){
			try {
				//BUG: sometime the lines will always null! Maybe BufferReader BUG in win10.
				List<String> lines = Files.readLines(file, Charset.forName("UTF-8"));
				for(String line : lines){
					String retValue = getValueFromLine(line,fieldName);
					if(retValue != null){
						return retValue;
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return null;
	}
	
	private static String getValueFromLine(String line,String fieldName) {
		if(line.contains("=")){
			String name = line.substring(0,line.lastIndexOf('='));
			String value = line.substring(line.lastIndexOf('=') + 1);
			if(name.equalsIgnoreCase(fieldName)){
				return value == null ? "" : value;
			}
		}
		return null;
	}
	
}
