package com.yaconfig.commands;

public class GetCommand extends Command{

	/**
	 * 
	 */
	private static final long serialVersionUID = -38186462140563862L;

	public GetCommand(String name) {
		super(name);
	}
	
	public byte[] executeQuery(String name){
		return exec.get(name);
	}
}
