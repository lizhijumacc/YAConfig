package com.yaconfig.client;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.yaconfig.client.exceptions.YAException;
import com.yaconfig.client.exceptions.YAFutureTimeoutException;
import com.yaconfig.client.exceptions.YAOperationErrorException;
import com.yaconfig.client.exceptions.YAServerDeadException;
import com.yaconfig.client.future.YAFuture;
import com.yaconfig.client.injector.DataFrom;
import com.yaconfig.client.message.YAMessage;
import com.yaconfig.client.message.YAMessageDecoder;
import com.yaconfig.client.message.YAMessageEncoder;
import com.yaconfig.client.message.YAMessageWrapper;
import com.yaconfig.client.watchers.EventType;
import com.yaconfig.client.watchers.RemoteWatchers;
import com.yaconfig.common.MessageProcessor;

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

public class YAConfigConnection extends MessageProcessor{
	
	private volatile Channel channel; 
	
	private volatile String connnectStr;
	
	private List<Node> nodes;
	
	private YAConfigConnection myself;
	
	private ScheduledExecutorService scheduleTask;
	
	private ConcurrentHashMap<Long,YAFuture<YAEntry>> futures;
	
	public static final int RECONNECT_WAIT = 5000;
	
	public static final int MAX_FUTURE_WAIT = 10000;
	
	public Object notifyConnected = new Object();
	
	private RemoteWatchers watchers;
	
	public YAConfigConnection(){
		this.myself = this;
	}
	
	public YAConfigConnection attach(String connStr){
		//should not change the connectStr
		if(this.connnectStr != null){
			return this;
		}
		this.connnectStr = connStr;
		futures = new ConcurrentHashMap<Long,YAFuture<YAEntry>>();
		this.nodes = new ArrayList<Node>();
		
		connectYAServer();
		return this;
	}
	
	public YAConfigConnection detach(){
		if(this.connnectStr == null){
			return this;
		}
		
		this.connnectStr = null;
		this.nodes = null;
		failAllFuture();
		channel.close();
		
		return this;
	}

	public void connectYAServer(){
		String[] split = this.connnectStr.split(",");
		for(String s : split){
			nodes.add(new Node(s));
		}
		
		scheduleTask = Executors.newSingleThreadScheduledExecutor();
		scheduleTask.scheduleAtFixedRate(new Runnable(){

			@Override
			public void run() {
				if(channel == null){
					connectServer();
				}
			}
			
		},0, RECONNECT_WAIT, TimeUnit.MILLISECONDS);
		
		scheduleTask.scheduleAtFixedRate(new Runnable(){

			@Override
			public void run() {
				purgeFutures();
			}
			
		},MAX_FUTURE_WAIT, MAX_FUTURE_WAIT, TimeUnit.MILLISECONDS);
		

	}
	
	protected void purgeFutures() {
		for(Entry<Long,YAFuture<YAEntry>> e : futures.entrySet()){
			YAFuture<YAEntry> f = e.getValue();
			long timeInterval = System.currentTimeMillis() - f.createTime;
			if(timeInterval > MAX_FUTURE_WAIT){
				f.setFailure(new YAFutureTimeoutException("Operation Timeout."));
				futures.remove(e.getKey());
			}
		}
	}

	protected void failAllFuture() {
		for(YAFuture<YAEntry> f : futures.values()){
			f.setFailure(new YAServerDeadException("Server Detached."));
		}
		futures.clear();
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
		  .option(ChannelOption.SO_REUSEADDR, true)
		  .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 2000);
		return boot.connect(host,port);	
	}

	public void processMessageImpl(Object msg) {
		YAMessageWrapper wrapper = (YAMessageWrapper)msg;
		YAMessage yamsg = wrapper.msg;
		
		if(yamsg.getType() == YAMessage.Type.ADD && watchers != null){
			watchers.notifyWatchers(yamsg.getKey(),EventType.ADD,DataFrom.REMOTE);
		}else if(yamsg.getType() == YAMessage.Type.DELETE && watchers != null){
			watchers.notifyWatchers(yamsg.getKey(),EventType.DELETE,DataFrom.REMOTE);
		}else if(yamsg.getType() == YAMessage.Type.UPDATE && watchers != null){
			watchers.notifyWatchers(yamsg.getKey(),EventType.UPDATE,DataFrom.REMOTE);
		}else if(yamsg.getType() == YAMessage.Type.ACK){
			
			YAFuture<YAEntry> f = futures.get(yamsg.getId());
			if(f != null){
				f.setSuccess(new YAEntry(yamsg.getKey(),yamsg.getValue()));
				futures.remove(yamsg.getId());
			}
		}else if(yamsg.getType() == YAMessage.Type.NACK){
			YAFuture<YAEntry> f = futures.get(yamsg.getId());
			if(f != null){
				f.setFailure(new YAOperationErrorException(new String(yamsg.getValue())));
				futures.remove(yamsg.getId());
			}
		}
	}
	
	public YAFuture<YAEntry> put(String key,byte[] value,int type){
		return writeCommand(key,value,type);
	}
	
	public YAFuture<YAEntry> get(String key,int type){
		return writeCommand(key,"".getBytes(),type);
	}
	
	public YAFuture<YAEntry> writeCommand(String key, byte[] bytes, int type) {
		if(futures == null){
			YAFuture<YAEntry> ref = new YAFuture<YAEntry>();
			ref.setFailure(new YAException("YAConfig in running in Local mode."));
			return ref;
		}
		
		YAMessage yamsg = new YAMessage(type,key,bytes);
		YAFuture<YAEntry> f = new YAFuture<YAEntry>();
		futures.putIfAbsent(yamsg.getId(), f);
		
		try {
			while(channel == null){
				synchronized(notifyConnected){
					notifyConnected.wait();
				}
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		produce(yamsg,channel.id());
		return f;
	}
	
	public void setChannel(Channel channel){
		this.channel = channel;
		if(this.channel != null && connnectStr != null && watchers != null){
			watchers.registerAllWatchers(connnectStr);
		}
	}

	public Channel getChannel(){
		return this.channel;
	}
	
	private void connectServer(){
		if(channel == null){
			synchronized(this){
				if(channel == null){
					for(Node n : nodes){
						ChannelFuture cf = connect0(n.getIp(),n.getPort()).awaitUninterruptibly();
						if(cf.isSuccess()){
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
				removeChannel(channel);
				setChannel(null);
				failAllFuture();
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
		super.channelActive(channel);
		setChannel(channel);
		synchronized(notifyConnected){
			notifyConnected.notifyAll();
		}
	}
	
	@Override
	public void channelInactive(Channel channel){
		channel.close();
		setChannel(null);
		super.channelInactive(channel);
	}

	public void setRemoteWatchers(RemoteWatchers watchers){
		this.watchers = watchers;
	}
}


