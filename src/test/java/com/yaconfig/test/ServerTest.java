package com.yaconfig.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.yaconfig.client.message.YAMessage;
import com.yaconfig.core.ConfigFactory;
import com.yaconfig.core.YAConfig;

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
    	
		/*client.watch("com.test.0", new WatcherListener(){

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
				client.get(key, YAMessage.Type.GET_LOCAL)
					.addListener(new FutureListener<YAEntry>(){

						@Override
						public void operationCompleted(AbstractFuture<YAEntry> future) {
							if(future.isSuccess()){
								try {
									System.out.println(future.get().getKey() + ": updated! value is :");
									System.out.println(new String(future.get().getValue()));
								} catch (InterruptedException | ExecutionException e) {
									e.printStackTrace();
								}
							}
						}
					
				});
			}
			
		});*/

    	YAConfig config = new YAConfig();
    	
    	config.scanPackage(ServerTest.class.getPackage().getName());
		TestConfig conf = (TestConfig)ConfigFactory.getConfig(TestConfig.class);
		
		long begin = System.currentTimeMillis();
		
		for(int i = 0;i < 100; i++){
			conf.setValue3("" + i);
		}
		
		long end = System.currentTimeMillis();
		
		System.out.println("usetime:" + (end - begin));
		
		//conf.useValue3FromFile();
		//Thread.sleep(3000);
		//conf.useValue3FromRemote();
		//conf.setValue3("fromMemory1111");
    	for(int i=0;i<0;i++){
    		//Thread.sleep(100);
    		/*YAFuture<YAEntry> f = client.put("com.test.0","fromRemote".getBytes(), YAMessage.Type.PUT_NOPROMISE);
    		
    		f.addListener(new FutureListener<YAEntry>(){

				@Override
				public void operationCompleted(AbstractFuture<YAEntry> f) {
					//System.out.println("put success!");
				}
    			
    		});
    		
    		if(i == 3){
    			client.attach("127.0.0.1:8888,127.0.0.1:8889,127.0.0.1:8890");
    			yaclient.unwatch("com.test.*");
    			System.out.println("attach1");
    		}
    		
       		if(i == 5){
    			client.detach();
    			System.out.println("detach1");
    		}
       		
       		if(i == 7){
    			client.attach("127.0.0.1:8888,127.0.0.1:8889,127.0.0.1:8890");
    			yaclient.unwatch("com.test.*");
    			System.out.println("attach2");
    		}*/
    	}
    	
    	/*for(int i=0;i<threadNum;i++){
    		cfs[i] = yaclient.connect0("127.0.0.1",8888).awaitUninterruptibly();
    		index = i;
    		if(cfs[i].isSuccess()){
    			service.execute(allinoneTask);
    		}
    	}*/
    	
		

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
