package com.yaconfig.client.message;

import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class YAMessageDecoder extends ByteToMessageDecoder{

	@Override
	protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
		int length = in.readInt();
		int type = in.readInt();
		int version = in.readInt();
		long id = in.readLong();
		int keyLength = in.readInt();
		byte[] keyBytes = new byte[keyLength];
		in.readBytes(keyBytes);
		
		int valueLength = in.readInt();
		byte[] valueBytes = new byte[valueLength];
		in.readBytes(valueBytes);
		
		YAMessage msg = new YAMessage(type,new String(keyBytes),valueBytes);
		msg.header.version = version;
		msg.header.id = id;
		
		YAMessageWrapper msgw = new YAMessageWrapper();
		msgw.ctx = ctx;
		msgw.msg = msg;

		out.add(msgw);
		
	}

}
