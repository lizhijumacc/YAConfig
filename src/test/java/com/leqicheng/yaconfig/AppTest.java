package com.leqicheng.yaconfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.yaconfig.message.YAMessage;
import com.yaconfig.message.YAMessageDecoder;
import com.yaconfig.message.YAMessageEncoder;
import com.yaconfig.message.YAMessageHeader;
import com.yaconfig.server.EndPoint;
import com.yaconfig.server.PeerDeadHandler;
import com.yaconfig.server.YAConfig;
import com.yaconfig.server.YAConfigClientHandler;
import com.yaconfig.server.YAConfigClient.ConnectionListener;

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
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
	
	public volatile boolean stop = false;
	
	public static AtomicLong sendCount = new AtomicLong(0);
	
	public static final int count = 1000000;
	
	ExecutorService service = Executors.newCachedThreadPool();
	
	public Runnable everyConnectTask;
	
	public Runnable allinoneTask;
	
	public ChannelFuture cf;
	
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
        
        everyConnectTask = new Runnable(){
        	@Override
        	public void run(){
        		
    			YAMessage yamsg = new YAMessage("com.test.test","qqqq".getBytes(),YAMessage.Type.PUT_NOPROMISE,sendCount.get());
    			try {
    				System.out.println(sendCount.get());
    				ChannelFuture f = connect();
    				if(f.isSuccess()){
        				AppTest.sendCount.incrementAndGet();
        				
        				f.channel().writeAndFlush(yamsg).sync();
        				f.channel().close().sync();
        				
        				
        				if(sendCount.get() > count){
        					stop = true;
        				}else{
        					service.execute(everyConnectTask);
        				}
    				}
    			} catch (InterruptedException e) {
    				// TODO Auto-generated catch block
    			}
        	}
        };
        
        allinoneTask = new Runnable(){
        	@Override
        	public void run(){
        		
    	    	for(int i=0;i<AppTest.count;i++){
    	    		YAMessageHeader header = new YAMessageHeader();
    	    		header.serverID = "6666";
    	    		header.sequenceNum = i;
    	    		header.type = YAMessage.Type.PUT_NOPROMISE;
    	    		header.serverStatus = EndPoint.Status.ALIVE;
    	    		
    	    		YAMessage yamsg = new YAMessage(header,"com.test.test","qqqq".getBytes());
    	    		try {
    	    			AppTest.sendCount.incrementAndGet();
    	    			System.out.println(sendCount.get());
    					cf.channel().writeAndFlush(yamsg).sync();
    				} catch (InterruptedException e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}
    	    	}	
    	    	
    	    	stop = true;
        	}
        };
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {	
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {	
    	
    	//service.execute(everyConnectTask);
    	
    	cf = connect();
    	if(cf.isSuccess()){
    		service.execute(allinoneTask);
    	}
    	
		long begin = System.currentTimeMillis() / 1000;

		while(true){
			
		}

		//long end = System.currentTimeMillis() / 1000;
		
		//System.out.println(sendCount.get() / (end - begin) + " QPS");
    }
    
    public ChannelFuture connect(){
    	final AppTest myself = this;
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
							 new LengthFieldPrepender(2),
							 new YAMessageEncoder()
						 );
		 	}
		}).option(ChannelOption.SO_KEEPALIVE,true)
		  .option(ChannelOption.TCP_NODELAY,true);
		return boot.connect("127.0.0.1",8888).awaitUninterruptibly();	
    }

}
