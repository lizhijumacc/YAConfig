package com.leqicheng.yaconfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.yaconfig.server.YAConfigClientHandler;
import com.yaconfig.server.YAConfigClient.ConnectionListener;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
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
	
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
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
    	final AppTest myself = this;
		Bootstrap boot = new Bootstrap();
		boot.group(new NioEventLoopGroup())
		.channel(NioSocketChannel.class)
		.handler(new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				ChannelPipeline p = ch.pipeline();
				p.addLast(
					 //new LengthFieldPrepender(2),
					 new ObjectEncoder(),
					 //new LengthFieldBasedFrameDecoder(65535,0,2,0,2),
					 new ObjectDecoder(ClassResolvers.cacheDisabled(null)),
					 new YAConfigTestClientHandler(myself));
		 	}
		}).option(ChannelOption.SO_KEEPALIVE,true)
		  .option(ChannelOption.TCP_NODELAY,true);
		boot.connect("127.0.0.1",8888).awaitUninterruptibly();	
		
		while(!stop){
			
		}
    }
}
