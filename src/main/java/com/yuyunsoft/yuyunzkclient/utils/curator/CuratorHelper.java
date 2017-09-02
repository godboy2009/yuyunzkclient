package com.yuyunsoft.yuyunzkclient.utils.curator;
  
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.data.Stat;
 
/**
 * 
 * 采用apache的curator来封装zookeeper方法
 * @author 黄小云
 * @date 2017-08-30
 * 
 */
public class CuratorHelper {
	   private static CuratorHelper instance = null;
	   
	   public static CuratorHelper getInstance() {
		   if (instance == null) 
			   instance = new CuratorHelper();
		   return instance;
	   }
	   
	   private CuratorFramework  curatorClient = null;	   
	   
	   
	   public CuratorFramework  connect(String host){
		
		   curatorClient =  CuratorFrameworkFactory.newClient(host, new RetryNTimes(10, 5000));  
		    
		   curatorClient.start();
	        System.out.println("zk client start successfully!");
	       return curatorClient;
	   }

	   /**
	    * 根据创建模式来创建路径对应的字符串数据
	    * @param path
	    * @param data
	    * @param createMode
	    * @throws Exception
	    */
	   public void create(String path, byte[] data) throws Exception { 
		   curatorClient.create().
           creatingParentsIfNeeded().
           forPath(path, data);
	   }
	   
	   /**
	    * 判断路径是否已经有重复
	    * @param path
	    * @return
	 * @throws Exception 
	    * @throws KeeperException
	    * @throws InterruptedException
	    */
	   public  Stat znode_exists(String path) throws Exception {
	      return curatorClient.checkExists().forPath(path);
	   }
	   
	   /**
	    * 修改已存在结点的数据
	    * @param path
	    * @param data
	    * @throws Exception  
	    */
	   public  void update(String path, byte[] data) throws Exception {
		   curatorClient.setData().forPath(path, data);
	   }
	   
	   /**
	    * 删除已存在的结点
	    * @param path
	    * @throws Exception  
	    */
	   public void delete(String path) throws Exception  {
		   curatorClient.delete().forPath(path);
		}
	   
	    /**
	     * 读取路径/path的数据
	     * @param path
	     * @return
	     * @throws Exception 
	     */
	    public byte[] getData( String path,Stat stat) throws Exception { 
	    	byte[] data = curatorClient.getData().forPath(path); 
	    	return data;
	     
	    }
	  
	   /**
	    * 关闭zookeeper
	    */
	   public void close(){
		  if (curatorClient != null) { 
			  curatorClient.close();
			  System.out.println("zookeeper集群关闭成功。");
		  }
			 
	   }
	 
	
		public static void main(String[] args) throws Exception {
			
			CuratorHelper.getInstance().connect("192.168.1.211:2181,192.168.1.212:2181,192.168.1.213:2181");
			
			Stat stat = CuratorHelper.getInstance().znode_exists("/ds-persistent"); // Stat checks the path of the znode
			
	         if(stat != null) {
	            System.out.println("结点已存在，其版本号为: " +
	            stat.getVersion());
	            
	            try {
                 byte[] bn = CuratorHelper.getInstance().getData("/ds-persistent",
                 stat);
                 String data = new String(bn,
                 "UTF-8");
                 System.out.println("/ds-persistent结点的数据为:" + data);
                 
                 CuratorHelper.getInstance().delete("/ds-persistent");
                 
                 System.out.println("删除/ds-persistent结点成功");
						
              } catch(Exception ex) {
                 System.out.println(ex.getMessage());
              }
	            
	            
	         } else {
	            System.out.println("Node does not exists");
	         }
			
			CuratorHelper.getInstance().create("/ds-persistent", "这是持久性结点数据".getBytes()); 
			
			CuratorHelper.getInstance().close();
			 
		}
}
