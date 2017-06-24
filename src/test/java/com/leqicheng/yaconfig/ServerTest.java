package com.leqicheng.yaconfig;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.yaconfig.client.AbstractFuture;
import com.yaconfig.client.FutureListener;
import com.yaconfig.client.Watcher;
import com.yaconfig.client.YAConfigClient;
import com.yaconfig.client.YAConfigValue;
import com.yaconfig.client.YAEntry;
import com.yaconfig.client.YAFuture;
import com.yaconfig.client.WatcherListener;
import com.yaconfig.client.message.YAMessage;

import io.netty.channel.ChannelFuture;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class ServerTest 
    extends TestCase
{
	
	public volatile boolean stop = false;
	
	public static volatile AtomicLong sendCount = new AtomicLong(0);
	
	ExecutorService service = Executors.newCachedThreadPool();
	
	public Runnable everyConnectTask;
	
	public Runnable allinoneTask;
	
	public int index = 0;
	
	public int threadNum = 2;
	
	public static final int count = 100;
	
	public ChannelFuture[] cfs = new ChannelFuture[threadNum];
	
	public YAConfigClient yaclient;

	public String remoteValue;
	
	public ServerTest myself;
	
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public ServerTest( String testName )
    {
        super( testName );
        
        myself = this;
        
        allinoneTask = new Runnable(){
        	@Override
        	public void run(){
        		
    	    	for(int i=0;i<ServerTest.count;i++){
    	    		
    	    		try {
						Thread.sleep(3000);
					} catch (InterruptedException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
    	    		
    	    		YAMessage yamsg = new YAMessage(YAMessage.Type.PUT_NOPROMISE,
    	    				"com.test." + (int)(Math.random()*1),String.valueOf((int)Math.random()*100).getBytes());
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
        return new TestSuite( ServerTest.class );
    }

    /**
     * Rigourous Test :-)
     * @throws InterruptedException 
     */
    public void testApp() throws InterruptedException
    {	
    	
    	//service.execute(everyConnectTask);
    	
    	yaclient = new YAConfigClient("127.0.0.1:8888,127.0.0.1:8889,127.0.0.1:8890",
    				ServerTest.class.getPackage().getName());
    	
		yaclient.watch("com.test.*", new WatcherListener(){

			@Override
			public void onDelete(Watcher w,String key) {
				//System.out.println(key + ": deleted!");
			}

			@Override
			public void onAdd(Watcher w,String key) {
				//System.out.println(key + ": added!");
			}

			@Override
			public void onUpdate(Watcher w,String key) {
				//System.out.println(key + ": updated!");
				yaclient.get(key, YAMessage.Type.GET_LOCAL)
					.addListener(new FutureListener<YAEntry>(){

						@Override
						public void operationCompleted(AbstractFuture<YAEntry> future) {
							if(future.isSuccess()){
								
							}
						}
					
				});
			}
			
		});    
    	
		TestConfig conf = new TestConfig(yaclient);
    	for(int i=0;i<10;i++){
    		Thread.sleep(2000);
    		YAFuture<YAEntry> f = yaclient.put("com.test." + (int)(Math.random()*5), 
    				"5656565".getBytes(), YAMessage.Type.PUT_NOPROMISE);
    		
    		f.addListener(new FutureListener<YAEntry>(){

				@Override
				public void operationCompleted(AbstractFuture<YAEntry> f) {
					System.out.println("put success!");
				}
    			
    		});
    		
    		System.out.println("TESTCONFIG VALUE: " + conf.value);
    		
    		/*if(i == 5){
    			yaclient.unwatch("com.test.*");
    			System.out.println("unwatch");
    		}*/
    	}
    	
    	/*for(int i=0;i<threadNum;i++){
    		cfs[i] = yaclient.connect0("127.0.0.1",8888).awaitUninterruptibly();
    		index = i;
    		if(cfs[i].isSuccess()){
    			service.execute(allinoneTask);
    		}
    	}*/
    	
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
