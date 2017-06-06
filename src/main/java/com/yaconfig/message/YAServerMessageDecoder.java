package com.yaconfig.message;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class YAServerMessageDecoder extends ByteToMessageDecoder{

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		
		int length = in.readInt();
		int version = in.readInt();
		int type = in.readInt();
		int serverStatus = in.readInt();
		long sequenceNum = in.readLong();
		String serverID = readString(in);
		
		YAServerMessageHeader header = new YAServerMessageHeader();
		header.version = version;
		header.type = type;
		header.serverStatus = serverStatus;
		header.sequenceNum = sequenceNum;
		header.serverID = serverID;
		
		String key = readString(in);
		byte[] value = readByte(in);
		
		YAServerMessage yamsg = new YAServerMessage(header,key,value);
		out.add(yamsg);

	}
	
	protected byte[] readByte(ByteBuf in){
		int length = in.readInt();
		byte[] b = new byte[length];
		in.readBytes(b);
		
		return b;
	}
	
	protected String readString(ByteBuf in){
		byte[] b = readByte(in);
		return new String(b);
	}

}
