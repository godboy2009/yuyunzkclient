package com.yuyunsoft.yuyunzkclient.utils.zkclient;

import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;

import com.github.zkclient.IZkChildListener;
import com.github.zkclient.ZkClient;

/**
 * 
 * 采用zkClient来封装zookeeper方法
 * 
 * @author 黄小云
 * @date 2017-08-30
 * 
 */
public class ZkClientHelper {
	private static ZkClientHelper instance = null;

	public static ZkClientHelper getInstance() {
		if (instance == null)
			instance = new ZkClientHelper();
		return instance;
	}

	ZkClient zkClient = null;

	public ZkClient connect(String host) throws IOException, InterruptedException {

		zkClient = new ZkClient("192.168.1.211:2181", 5000);
		System.out.println("ZK 成功建立连接！");

		String path = "/zk-test";
		// 注册子节点变更监听（此时path节点并不存在，但可以进行监听注册）
		zkClient.subscribeChildChanges(path, new IZkChildListener() {
			public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
				System.out.println("路径" + parentPath + "下面的子节点变更。子节点为：" + currentChilds);
			}
		});

		// 递归创建子节点（此时父节点并不存在）
		zkClient.createPersistent("/zk-test/a1", true);
		Thread.sleep(5000);
		System.out.println(zkClient.getChildren(path));

		return zkClient;
	}

	/**
	 * 根据创建模式来创建路径对应的字符串数据
	 * 
	 * @param path
	 * @param data
	 * @param createMode
	 * 
	 */
	public void create(String path, byte[] data, CreateMode createMode) {
		zkClient.create(path, data, createMode);
	}

	/**
	 * 判断路径是否已经有重复
	 * 
	 * @param path
	 * @return
	 */
	public boolean znode_exists(String path) {
		return zkClient.exists(path);
	}

	/**
	 * 修改已存在结点的数据
	 * 
	 * @param path
	 * @param data
	 */
	public void update(String path, byte[] data) {
		zkClient.writeData(path, data);
	}

	/**
	 * 删除已存在的结点
	 * 
	 * @param path
	 */
	public void delete(String path) {
		zkClient.delete(path);
	}

	/**
	 * <p>
	 * 获取某个节点下的所有子节点,List getChildren(path<节点路径>, watcher<监视器>)该方法有多个重载
	 * </p>
	 * 
	 * @param path
	 *            zNode节点路径
	 * @return 子节点路径集合 说明,这里返回的值为节点名
	 * 
	 *         <pre>
	 *     eg.
	 *     /node
	 *     /node/child1
	 *     /node/child2
	 *     getChild( "node" )户的集合中的值为["child1","child2"]
	 *         </pre>
	 * 
	 */
	public List<String> getChild(String path) {

		List<String> list = zkClient.getChildren(path);
		if (list.isEmpty()) {
			System.out.println("中没有节点" + path);
		}
		return list;

	}

	/**
	 * 读取路径/path的数据
	 * 
	 * @param path
	 * @return
	 */
	public byte[] getData(String path) {

		byte[] data = zkClient.readData(path);
		return data;

	}

	/**
	 * 关闭zookeeper
	 */
	public void close() {
		if (zkClient != null) {
			zkClient.close();
			System.out.println("zookeeper集群关闭成功。");
		}
	}

	public static void main(String[] args) throws IOException, InterruptedException {

		ZkClientHelper.getInstance().connect("192.168.1.211:2181,192.168.1.212:2181,192.168.1.213:2181");

		Boolean bolNodeExist = ZkClientHelper.getInstance().znode_exists("/ds-persiste");

		if (bolNodeExist) {
			System.out.println("结点已存在");

			try {
				byte[] bn = ZkClientHelper.getInstance().getData("/ds-persiste");
				String data = new String(bn, "UTF-8");
				System.out.println("/ds-persistent结点的数据为:" + data);

				ZkClientHelper.getInstance().delete("/ds-persistent");

				System.out.println("删除/ds-persistent结点成功");

			} catch (Exception ex) {
				System.out.println(ex.getMessage());
			}

		} else {
			System.out.println("Node does not exists");
		}

		ZkClientHelper.getInstance().create("/ds-persistent", "这是持久性结点数据".getBytes(), CreateMode.PERSISTENT);
 

		ZkClientHelper.getInstance().close();

	}

}
