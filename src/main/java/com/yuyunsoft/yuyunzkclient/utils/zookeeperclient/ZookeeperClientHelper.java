package com.yuyunsoft.yuyunzkclient.utils.zookeeperclient;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * 采用zookeeper的客户端提供的jar包来封装zookeeper方法
 * @author 黄小云
 * @date 2017-08-30
 *
 */
public class ZookeeperClientHelper{
	
	   private static ZookeeperClientHelper instance = null;
	   
	   public static ZookeeperClientHelper getInstance() {
		   if (instance == null) 
			   instance = new ZookeeperClientHelper();
		   return instance;
	   }
	   
	   private ZooKeeper zookeeper = null;	   
	   
	   private final CountDownLatch zkClientCountDownLatch = new CountDownLatch(1);

	   
	   public ZooKeeper connect(String host) throws IOException{
		
		   zookeeper = new ZooKeeper(host,5000,new Watcher() {
			
	         public void process(WatchedEvent event) {

	     		if (KeeperState.SyncConnected == event.getState()) {
	    			zkClientCountDownLatch.countDown();
	    			
	    			System.out.println("zookeeper集群的连接状态是:" + event.getState());
	    		}
	         }
	      });
		   
		  System.out.println("zookeeper集群的连接状态是:" + zookeeper.getState());
			
			try{
				zkClientCountDownLatch.await();
			}catch(InterruptedException e) { 
				System.out.println("zookeeper集群连接失败,原因可能是:" + e.getMessage());
			}
	      
	      return zookeeper;
	   }

	   /**
	    * 根据创建模式来创建路径对应的字符串数据
	    * @param path
	    * @param data
	    * @param createMode
	    * @throws KeeperException
	    * @throws InterruptedException
	    */
	   public void create(String path, byte[] data,CreateMode createMode) throws 
	      KeeperException,InterruptedException {
		   zookeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE,
				   createMode);
	   }
	   
	   /**
	    * 判断路径是否已经有重复
	    * @param path
	    * @return
	    * @throws KeeperException
	    * @throws InterruptedException
	    */
	   public  Stat znode_exists(String path) throws
	      KeeperException,InterruptedException {
	      return zookeeper.exists(path, true);
	   }
	   
	   /**
	    * 修改已存在结点的数据
	    * @param path
	    * @param data
	    * @throws KeeperException
	    * @throws InterruptedException
	    */
	   public  void update(String path, byte[] data) throws
	      KeeperException,InterruptedException {
		   zookeeper.setData(path, data, zookeeper.exists(path,true).getVersion());
	   }
	   
	   /**
	    * 删除已存在的结点
	    * @param path
	    * @throws KeeperException
	    * @throws InterruptedException
	    */
	   public void delete(String path) throws KeeperException,InterruptedException {
		   zookeeper.delete(path,zookeeper.exists(path,true).getVersion());
		}
	   
	    /**
	     * 读取路径/path的数据
	     * @param path
	     * @return
	     * @throws InterruptedException 
	     * @throws KeeperException 
	     */
	    public byte[] getData( String path,Stat stat) throws KeeperException, InterruptedException{
		     
	    	byte[] data = zookeeper.getData(path,false,stat); 
           return data;
	     
	    }
	  
	   /**
	    * 关闭zookeeper
	    */
	   public void close(){
		  if (zookeeper != null)
			try {
				zookeeper.close();
				System.out.println("zookeeper集群关闭成功。");
			} catch (InterruptedException e) { 
				System.out.println("zookeeper集群关闭失败,原因可能是:" + e.getMessage());
			}
	   }
	 
	
		public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
			
			ZookeeperClientHelper.getInstance().connect("192.168.1.211:2181,192.168.1.212:2181,192.168.1.213:2181");
			
			Stat stat = ZookeeperClientHelper.getInstance().znode_exists("/ds-persistent"); // Stat checks the path of the znode
			
	         if(stat != null) {
	            System.out.println("结点已存在，其版本号为: " +
	            stat.getVersion());
	            
	            try {
                    byte[] bn = ZookeeperClientHelper.getInstance().getData("/ds-persistent",
                    stat);
                    String data = new String(bn,
                    "UTF-8");
                    System.out.println("/ds-persistent结点的数据为:" + data);
                    
                    ZookeeperClientHelper.getInstance().zookeeper.delete("/ds-persistent", stat.getVersion());
                    
                    System.out.println("删除/ds-persistent结点成功");
						
                 } catch(Exception ex) {
                    System.out.println(ex.getMessage());
                 }
	            
	            
	         } else {
	            System.out.println("Node does not exists");
	            ZookeeperClientHelper.getInstance().create("/ds-persistent", "这是持久性结点数据".getBytes(), CreateMode.PERSISTENT);
	         } 
 
//			ZookeeperClientHelper.getInstance().create("/ds-persistent-seq", "这是持久顺序结点数据".getBytes(), CreateMode.PERSISTENT_SEQUENTIAL);
//			ZookeeperClientHelper.getInstance().create("/ds-temp", "这是临时性结点数据".getBytes(), CreateMode.EPHEMERAL);
//			ZookeeperClientHelper.getInstance().create("/ds-temp-seq", "这是临时性顺序结点数据".getBytes(), CreateMode.EPHEMERAL_SEQUENTIAL);
			
			ZookeeperClientHelper.getInstance().close();
			 
		}

}
