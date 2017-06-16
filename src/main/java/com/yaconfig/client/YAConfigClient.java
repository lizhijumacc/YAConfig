package com.yaconfig.client;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.yaconfig.client.message.YAMessage;
import com.yaconfig.client.message.YAMessageDecoder;
import com.yaconfig.client.message.YAMessageEncoder;
import com.yaconfig.client.message.YAMessageWrapper;
import com.yaconfig.server.MessageProcessor;

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
	
	private volatile Channel channel; 
	
	private String connnectStr;
	
	private List<Node> nodes;
	
	private HashMap<String,Watcher> watchers;
	
	private YAConfigClient myself;
	
	private ScheduledExecutorService keepaliveTask;
	
	private Object isWritable = new Object();
	
	public YAConfigClient(String connStr){
		this.nodes = new ArrayList<Node>();
		this.watchers = new HashMap<String,Watcher>();
		this.myself = this;
		this.connnectStr = connStr;
		
		String[] split = connStr.split(",");
		for(String s : split){
			nodes.add(new Node(s));
		}
		
		keepaliveTask = Executors.newSingleThreadScheduledExecutor();
		keepaliveTask.scheduleAtFixedRate(new Runnable(){

			@Override
			public void run() {
				if(channel == null){
					connectServer();
				}
			}
			
		},0, 3, TimeUnit.SECONDS);
	}
	
	private ChannelFuture connect0(String host,int port){
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
							 new YAConfigClientHandler(myself),
							 new LengthFieldPrepender(2),
							 new YAMessageEncoder()
						 );
		 	}
		}).option(ChannelOption.SO_KEEPALIVE,true)
		  .option(ChannelOption.TCP_NODELAY,true)
		  .option(ChannelOption.SO_REUSEADDR, true);
		return boot.connect(host,port);	
	}

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
	
	public void put(String key,byte[] value,int type){
		
		if(checkChannel()){
			produce(new YAMessage(type,key,value),channel.id());
		}else{
			try {
				synchronized(isWritable){
					isWritable.wait();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			produce(new YAMessage(type,key,value),channel.id());
		}
	}
	
	public void get(String key,int type){
		
		if(checkChannel()){
			produce(new YAMessage(type,key,"".getBytes()),channel.id());
		}else{
			try {
				synchronized(isWritable){
					isWritable.wait();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			produce(new YAMessage(type,key,"".getBytes()),channel.id());
		}
	}
	
	public void watch(String key,WatcherListener... listeners){
		
		if(checkChannel()){
			watch0(key,channel,listeners);
		}else{
			try {
				synchronized(isWritable){
					isWritable.wait();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			watch0(key,channel,listeners);
		}
	}
	
	public void unwatch(String key){
		
		synchronized(this.watchers){
			watchers.remove(key);
		}
		if(checkChannel()){
			produce(new YAMessage(YAMessage.Type.UNWATCH,key,"".getBytes()),channel.id());
		}else{
			try {
				synchronized(isWritable){
					isWritable.wait();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			produce(new YAMessage(YAMessage.Type.UNWATCH,key,"".getBytes()),channel.id());
		}
	}
	
	private void watch0(String key,Channel channel,WatcherListener... listeners){
		produce(new YAMessage(YAMessage.Type.WATCH,key,"".getBytes()),channel.id());
		watchLocal(key,listeners);
	}
	
	private void watchLocal(String key,WatcherListener... listeners){
		if(watchers.containsKey(key)){
			watchers.get(key).addListeners(listeners);
		}else{
			Watcher watcher = new Watcher(key,channel);
			watcher.addListeners(listeners);
			synchronized(this.watchers){
				watchers.put(key, watcher);
			}
		}
	}
	
	public void setChannel(Channel channel){
		this.channel = channel;
	
		if(this.channel != null){
			registerWatchers();
		}
	}
	
	private void registerWatchers() {
		for(Watcher w : watchers.values()){
			produce(new YAMessage(YAMessage.Type.WATCH,w.getKey(),"".getBytes()),channel.id());
		}
	}

	public Channel getChannel(){
		return this.channel;
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
	
	private void connectServer(){
		if(channel == null){
			synchronized(this){
				if(channel == null){
					for(Node n : nodes){
						ChannelFuture cf = connect0(n.getIp(),n.getPort());
						boolean isDone = cf.awaitUninterruptibly(2, TimeUnit.SECONDS);
						if(isDone && cf.isSuccess()){
							break;
						}else{
							cf.channel().close();
						}
					}
				}
			}
		} else {
			Socket socket = new Socket();
			try {
				socket.setReuseAddress(true);
				socket.connect(channel.remoteAddress());
			} catch (IOException e) {
				setChannel(null);
				System.out.println("current server is inactive,try to connect another one.");
			} finally {
				try {
					socket.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	public boolean checkChannel(){
		return channel != null && channel.isWritable() && channel.isActive();
	}
	
	@Override
	public void exceptionCaught(Channel channel, Throwable cause){
		if(null != channel){
			channel.close();
			setChannel(null);
			try {
				super.exceptionCaught(channel, cause);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void channelActive(Channel channel){
		setChannel(channel);
		notifyWritable();
		super.channelActive(channel);
	}
	
	@Override
	public void channelInactive(Channel channel){
		channel.close();
		setChannel(null);
		super.channelInactive(channel);
	}
	
	public void notifyWritable(){
		synchronized(isWritable){
			isWritable.notifyAll();
		}
	}
	
	
}


