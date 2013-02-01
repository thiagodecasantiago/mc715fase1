
import java.io.IOException;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;


public class Main {

    private static List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    private static ZooKeeper zk;
    private static String ElectionNode = "/election";
    private static String StringID;
    static Integer mutex = -1;

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
	     try {
	       String connectString = args[0]+":41111";
	       zk = new ZooKeeper(connectString,4000, null);
	       Stat ElectionNodeStat = zk.exists(ElectionNode, null);
	       if (ElectionNodeStat == null){
	    	   zk.create(ElectionNode, new byte[0], acl, CreateMode.PERSISTENT);
	       }
	       beLeader();
	       Thread.sleep(Integer.valueOf(args[1]));
    	   	       
	       
	     } catch (Exception ex) {
	       System.out.println("Uso: java -jar EleicaoLider host segundos");				
	     }
}
	
    private static void beLeader(){
    	try {
    		String ProposalNode = ElectionNode + "/proposal-n_";
    		StringID = zk.create(ProposalNode, new byte[0], acl, CreateMode.EPHEMERAL_SEQUENTIAL);
    		StringID = StringID.substring(21);
    		System.out.println("Criado proposta de lider numero "+StringID+".");
    		checkIfLeader();
    	} catch (KeeperException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	} catch (InterruptedException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}

    }

    private static void checkIfLeader(){
    	String ProposalNode = ElectionNode + "/proposal-n_";
    	try {
    		for (Integer i = Integer.valueOf(StringID) - 1; i>= 0; i--){
    			String VizinhoNode = ProposalNode + String.format("%010d", i);
    			if (zk.exists(VizinhoNode,false) != null) {
    				zk.getChildren(VizinhoNode , new Watcher(){
						@Override
						public void process(WatchedEvent event) { 
							if (event.getType() == Watcher.Event.EventType.NodeDeleted) checkIfLeader();
						}});
    				System.out.println("Vigiando node "+VizinhoNode);
    				//System.out.println("Lider atual e: "+ zk.getData(ElectionNode, false, new Stat()).toString());
    				return;
    			}
    		}
    		zk.setData(ElectionNode, StringID.getBytes(), -1);
    		System.out.println("Eu, proposta "+StringID+", sou o lider! \\o/ \\o/.");				
    	} catch (KeeperException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	} catch (InterruptedException e) {
    		// TODO Auto-generated catch block
    		e.printStackTrace();
    	}
    }
	
}
