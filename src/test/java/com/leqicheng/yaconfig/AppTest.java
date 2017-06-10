package com.leqicheng.yaconfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.yaconfig.message.YAMessage;
import com.yaconfig.message.YAMessageDecoder;
import com.yaconfig.message.YAMessageEncoder;
import com.yaconfig.message.YAServerMessage;
import com.yaconfig.message.YAServerMessageDecoder;
import com.yaconfig.message.YAServerMessageEncoder;
import com.yaconfig.message.YAServerMessageHeader;
import com.yaconfig.server.EndPoint;
import com.yaconfig.server.PeerDeadHandler;
import com.yaconfig.server.YAConfig;
import com.yaconfig.server.YAConfigMessageHandler;
import com.yaconfig.server.YAConfigAcceptor.ConnectionListener;

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
	
	public static volatile AtomicLong sendCount = new AtomicLong(0);
	
	ExecutorService service = Executors.newCachedThreadPool();
	
	public Runnable everyConnectTask;
	
	public Runnable allinoneTask;
	
	public int index = 0;
	
	public int threadNum = 1;
	
	public static final int count = 1;
	
	public ChannelFuture[] cfs = new ChannelFuture[threadNum];
	
	public ChannelFuture watcherCF;
	
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
        
        allinoneTask = new Runnable(){
        	@Override
        	public void run(){
        		
    	    	for(int i=0;i<AppTest.count;i++){
    	    		
    	    		YAMessage yamsg = new YAMessage(YAMessage.Type.PUT_NOPROMISE,
    	    				"com.test.test","qqqq".getBytes());
    	    		try {
    	    			System.out.println(sendCount.incrementAndGet());
    					cfs[index].channel().writeAndFlush(yamsg).sync();
    				} catch (InterruptedException e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}
    	    	}	
    	    	
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
     * @throws InterruptedException 
     */
    public void testApp() throws InterruptedException
    {	
    	
    	//service.execute(everyConnectTask);
    	
    	watcherCF = connect();
    	
    	if(watcherCF.isSuccess()){
    		System.out.println("??????");
    		YAMessage msg = new YAMessage(YAMessage.Type.WATCH,"com.test.test","".getBytes());
    		watcherCF.channel().writeAndFlush(msg);
    		
    		Thread.sleep(2000);
    		
        	for(int i=0;i<threadNum;i++){
        		cfs[i] = connect();
        		index = i;
        		if(cfs[i].isSuccess()){
        			service.execute(allinoneTask);
        		}
        	}
    	}
    	
		long begin = System.currentTimeMillis() / 1000;

		while(true){
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
							 new YAConfigTestClientHandler(myself),
							 new LengthFieldPrepender(2),
							 new YAMessageEncoder()
						 );
		 	}
		}).option(ChannelOption.SO_KEEPALIVE,true)
		  .option(ChannelOption.TCP_NODELAY,true);
		return boot.connect("127.0.0.1",8888).awaitUninterruptibly();	
    }

}
