package com.yaconfig.server.message;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class YAServerMessageEncoder extends MessageToByteEncoder<YAServerMessage>{

	@Override
	protected void encode(ChannelHandlerContext ctx, YAServerMessage msg, ByteBuf out) throws Exception {
		YAServerMessageHeader header = msg.header;
		
		out.writeInt(msg.length());
		out.writeInt(header.version);
		out.writeInt(header.type);
		out.writeInt(header.serverStatus);
		out.writeLong(header.sequenceNum);
		byte[] serverID = header.serverID.getBytes();
		out.writeInt(serverID.length);
		out.writeBytes(serverID);
		
		byte[] key = msg.key.getBytes();
		out.writeInt(key.length);
		out.writeBytes(key);
		
		byte[] value = msg.value;
		out.writeInt(value.length);
		out.writeBytes(value);

	}

}
