package com.yaconfig.client;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.yaconfig.client.message.YAMessage;
import com.yaconfig.client.message.YAMessageDecoder;
import com.yaconfig.client.message.YAMessageEncoder;
import com.yaconfig.client.message.YAMessageWrapper;
import com.yaconfig.server.MessageProcessor;
import com.yaconfig.server.YAConfig;
import com.yaconfig.server.YAConfigMessageHandler;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;

public class YAConfigClient extends MessageProcessor{
	
	private volatile Channel watcherChannel; 
	
	private String connnectStr;
	
	private List<Node> nodes;
	
	private HashMap<String,Watcher> watchers;
	
	private YAConfigClient myself;
	
	public YAConfigClient(String connStr){
		this.nodes = new ArrayList<Node>();
		this.watchers = new HashMap<String,Watcher>();
		this.myself = this;
		this.connnectStr = connStr;
		
		String[] split = connStr.split(",");
		for(String s : split){
			nodes.add(new Node(s));
		}
	}
	
	public ChannelFuture connect0(String host,int port){
		Bootstrap boot = new Bootstrap();
		boot.group(new NioEventLoopGroup())
		.channel(NioSocketChannel.class)
		.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				ChannelPipeline p = ch.pipeline();
				p.addLast(
							 new LengthFieldBasedFrameDecoder(65535,0,2,0,2),
							 new YAMessageDecoder(),
							 new YAConfigMessageHandler(myself),
							 new LengthFieldPrepender(2),
							 new YAMessageEncoder()
						 );
		 	}
		}).option(ChannelOption.SO_KEEPALIVE,true)
		  .option(ChannelOption.TCP_NODELAY,true);
		return boot.connect(host,port);	
	}

	@Override
	public void processMessageImpl(Object msg) {
		YAMessageWrapper wrapper = (YAMessageWrapper)msg;
		YAMessage yamsg = wrapper.msg;
		if(yamsg.getType() == YAMessage.Type.ADD){
			notifyWatchers(yamsg.getKey(),YAMessage.Type.ADD);
		}else if(yamsg.getType() == YAMessage.Type.DELETE){
			notifyWatchers(yamsg.getKey(),YAMessage.Type.DELETE);
		}else if(yamsg.getType() == YAMessage.Type.UPDATE){
			notifyWatchers(yamsg.getKey(),YAMessage.Type.UPDATE);
		}
	}
	
	public void watch(String key,WatcherListener... listeners){
		if(watcherChannel == null){
			ChannelFuture wcf = connect0("127.0.0.1",8888).syncUninterruptibly();
			if(wcf.isSuccess()){
				watcherChannel = wcf.channel();
				watch0(key,watcherChannel,listeners);
			}
		}else{
			watch0(key,watcherChannel,listeners);
		}
	}
	
	private void watch0(String key,Channel channel,WatcherListener... listeners){
		if(channel.isActive() && channel.isWritable()){
			YAMessage watcherMsg = new YAMessage(YAMessage.Type.WATCH,key,"".getBytes());
			ChannelFuture cf = channel.writeAndFlush(watcherMsg).awaitUninterruptibly();
			if(cf.isSuccess()){
				watchLocal(key,listeners);
			}
		}
	}
	
	private void watchLocal(String key,WatcherListener... listeners){
		if(watchers.containsKey(key)){
			watchers.get(key).addListeners(listeners);
		}else{
			Watcher watcher = new Watcher(key,watcherChannel);
			watcher.addListeners(listeners);
			synchronized(this.watchers){
				watchers.put(key, watcher);
			}
		}
	}
	
	public void unwatch(String key){
		synchronized(this.watchers){
			watchers.remove(key);
		}
		
		if(watcherChannel != null 
				&& watcherChannel.isActive() 
				&& watcherChannel.isWritable()){
			watcherChannel.writeAndFlush(new YAMessage(YAMessage.Type.UNWATCH,key,"".getBytes()));
		}
	}
	
	public void notifyWatchers(String key,int event){
		synchronized(this.watchers){
			for(Watcher w: watchers.values()){
				if(key.matches(w.getKey())){
					w.notifyListeners(event,key);
				}
			}
		}
	}
}
