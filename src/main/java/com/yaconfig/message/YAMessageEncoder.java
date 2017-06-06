package com.yaconfig.message;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class YAMessageEncoder extends MessageToByteEncoder<YAMessage>{

	@Override
	protected void encode(ChannelHandlerContext ctx, YAMessage msg, ByteBuf out) throws Exception {
		out.writeInt(msg.length());
		out.writeInt(msg.header.type);
		out.writeInt(msg.header.version);
		out.writeInt(msg.getKey().getBytes().length);
		out.writeBytes(msg.getKey().getBytes());
		out.writeInt(msg.getValue().length);
		out.writeBytes(msg.getValue());
	}

}
