package com.yaconfig.server.commands;
public class PutCommand extends Command{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5378136152411869587L;
	
	public PutCommand(String name) {
		super(name);
	}

	public void execute(String key,byte[] value,boolean withoutpromise) {
		exec.put(key,value,withoutpromise);
	}

}
