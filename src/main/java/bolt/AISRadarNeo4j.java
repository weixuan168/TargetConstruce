package bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import entity.Labels;
import entity.Relations;
import net.sf.json.JSONObject;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import util.Neo4jDao;

import java.util.Map;

public class AISRadarNeo4j extends BaseBasicBolt {
    private GraphDatabaseService graphDB;
    private TraversalDescription getTarget;
    private TraversalDescription getTracks;

    public void prepare(Map stormConf, TopologyContext context) {
        graphDB = Neo4jDao.getInstance().getGraphDB();
        getTarget = graphDB.traversalDescription()
                .relationships(Relations.CONTAINS, Direction.INCOMING)
                .evaluator(Evaluators.atDepth(1));
        getTracks = graphDB.traversalDescription()
                .relationships(Relations.CONTAINS, Direction.OUTGOING)
                .evaluator(Evaluators.atDepth(1));
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        JSONObject jsonObject = (JSONObject) tuple.getValue(0);
        String ais_rowkey = jsonObject.getString("ais_rowkey");
        String radar_rowkey = jsonObject.getString("radar_rowkey");
        String mmsi = jsonObject.getString("mmsi");
        long ais_startTime = jsonObject.getLong("ais_startTime");
        long ais_endTime = jsonObject.getLong("ais_endTime");
        long radar_startTime = jsonObject.getLong("radar_startTime");
        long radar_endTime = jsonObject.getLong("radar_endTime");

        try (Transaction tx = graphDB.beginTx()) {
            //找标签是ais_track，属性是rowkey，值为ais_rowkey的节点
            ResourceIterator<Node> aisTracks = graphDB.findNodes(Labels.ais_track, "rowkey", ais_rowkey);
            Node aisTrack_old = null;
            if (aisTracks.hasNext()) {
                aisTrack_old = aisTracks.next();
            }
            //找标签是radar_track,属性是rowkey，值为radar_rowkey的节点
            ResourceIterator<Node> radarTracks = graphDB.findNodes(Labels.radar_track, "rowkey", radar_rowkey);
            Node radarTrack_old = null;
            if (radarTracks.hasNext()) {
                radarTrack_old = radarTracks.next();
            }
            //1. ais键、雷达键都不存在
            //todo:有点问题
            if (aisTrack_old == null && radarTrack_old == null) {

                //新建一个target类型节点
                Node target = graphDB.createNode(Labels.target);

                //新建一个ais_track类型节点，属性是rowkey
                Node aisTrack = graphDB.createNode(Labels.ais_track);
                aisTrack.setProperty("rowkey", ais_rowkey);

                //新建一个radar_track类型节点，属性是rowkey
                Node radarTrack = graphDB.createNode(Labels.radar_track);
                radarTrack.setProperty("rowkey", radar_rowkey);

                //新建一个property类型节点，属性是mmsi
                Node property = graphDB.createNode(Labels.property);
                property.setProperty("mmsi", mmsi);

                //建立target节点到ais_track节点的关系：CONTAINS,属性是轨迹段的开始和结束时间
                Relationship r1 = target.createRelationshipTo(aisTrack, Relations.CONTAINS);
                r1.setProperty("start_time", ais_startTime);
                r1.setProperty("end_time", ais_endTime);

                //建立target节点到radar_track节点的关系：CONTAINS,属性是轨迹段的开始和结束时间
                Relationship r2 = target.createRelationshipTo(radarTrack, Relations.CONTAINS);
                r2.setProperty("start_time", radar_startTime);
                r2.setProperty("end_time", radar_endTime);

                //建立target节点到mmsi属性节点的关系：HAS
                Relationship r3 = target.createRelationshipTo(property, Relations.HAS);
            }
            //2. ais键存在，雷达键不存在
            else {
                if (aisTrack_old != null && radarTrack_old == null) {
                    //找到ais节点所在的target
                    Iterable<Node> Targets = getTarget.traverse(aisTrack_old).nodes();
                    Node target = null;
                    for (Node node : Targets) {
                        target = node;
                    }
                    //找到该target连接的radar节点
                    Node radarTrack_previous = null;
                    Iterable<Node> Tracks = getTracks.traverse(target).nodes();
                    for (Node track : Tracks) {
                        for (Label label : track.getLabels()) {
                            if (label.name().equals("radar_track")) {
                                radarTrack_previous = track;
                            }
                        }
                    }
                    //新建一个radar_track类型节点，属性是rowkey
                    Node radarTrack = graphDB.createNode(Labels.radar_track);
                    radarTrack.setProperty("rowkey", radar_rowkey);

                    //建立该节点到radarTrack_previous节点的关系：PREVIOUS,属性是轨迹段的开始和结束时间
                    Relationship r = radarTrack.createRelationshipTo(radarTrack_previous, Relations.CONTAINS);

                    //删除target节点到radarTrack_previous节点的关系：CONTAINS

                    //建立target节点到radarTrack节点的关系：CONTAINS，属性是轨迹段的开始和结束时间
                    Relationship r2 = target.createRelationshipTo(radarTrack, Relations.CONTAINS);
                    r2.setProperty("start_time", ais_startTime);
                    r2.setProperty("end_time", ais_endTime);
                }
                //3. ais键不存在，雷达键存在
                else if (aisTrack_old == null && radarTrack_old != null) {

                }
                //4. ais键、雷达键都存在
                else if (aisTrack_old != null && radarTrack_old != null) {

                }
            }
            tx.success();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("AISRadarNeo4j"));
    }
}
