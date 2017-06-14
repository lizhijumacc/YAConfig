package com.leqicheng.yaconfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.yaconfig.client.Watcher;
import com.yaconfig.client.YAConfigClient;
import com.yaconfig.client.WatcherListener;
import com.yaconfig.client.message.YAMessage;

import io.netty.channel.ChannelFuture;
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
	
	public int threadNum = 10;
	
	public static final int count = 10;
	
	public ChannelFuture[] cfs = new ChannelFuture[threadNum];
	
	public YAConfigClient yaclient;
	
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
    	    				"com.test." + (int)(Math.random()*10),"qqqq".getBytes());
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
    	
    	yaclient = new YAConfigClient("127.0.0.1:8888,127.0.0.1:8889,127.0.0.1:8890");
    	
    	yaclient.watch("com.test.*", new WatcherListener(){

			@Override
			public void onDelete(Watcher w,String key) {
				System.out.println(key + ": deleted!");
			}

			@Override
			public void onAdd(Watcher w,String key) {
				System.out.println(key + ": added!");
			}

			@Override
			public void onUpdate(Watcher w,String key) {
				System.out.println(key + ": updated!");
			}
    		
    	});
    	
    	Thread.sleep(3000);
    	
    	for(int i=0;i<threadNum;i++){
    		cfs[i] = yaclient.connect0("127.0.0.1",8888).awaitUninterruptibly();
    		index = i;
    		if(cfs[i].isSuccess()){
    			service.execute(allinoneTask);
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

}
