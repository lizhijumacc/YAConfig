package com.yaconfig.commands;

import com.yaconfig.server.YAMessage;

public class PutCommand extends Command{

	public PutCallback callback;
	
	public PutCommand(String name) {
		super(name);
	}

	public void execute(String key,byte[] value) {
		exec.put(new YAMessage(key,value,YAMessage.Type.PUT),callback);
	}

	public void setCallback(PutCallback putCallback) {
		callback = putCallback;
	}

	public void execute(YAMessage yamsg) {
		exec.put(yamsg,callback);
	}
}
