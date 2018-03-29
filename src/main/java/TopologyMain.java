import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import bolt.AISElespFusion;
import bolt.AISRadarFusion;
import bolt.AISRadarNeo4j;
import spout.ReadAISTrack;

/**
 * Created by Xuan on 2018/3/29.
 */
public class TopologyMain {
    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("ReadAISTrack", new ReadAISTrack());
        builder.setBolt("AISRadarFusion", new AISRadarFusion()).shuffleGrouping("ReadAISTrack");
        builder.setBolt("AISElespFusion", new AISElespFusion()).shuffleGrouping("ReadAISTrack");
        builder.setBolt("AISRadarNeo4j",new AISRadarNeo4j()).shuffleGrouping("AISRadarFusion");

        Config conf = new Config();
        conf.setMaxSpoutPending(10);
        //开发阶段设置debug属性为true,Storm会打印节点间交换的所有消息，及其它调试数据
        conf.setDebug(true);
        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting-Started-Toplogie", conf, builder.createTopology());
    }
}
