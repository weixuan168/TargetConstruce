package bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import entity.Labels;
import entity.Relations;
import net.sf.json.JSONObject;
import org.neo4j.graphdb.*;
import util.Neo4jDao;

import java.util.ArrayList;
import java.util.Map;

public class AISRadarNeo4j extends BaseBasicBolt {
    private GraphDatabaseService graphDb;

    public void prepare(Map stormConf, TopologyContext context) {
        graphDb = Neo4jDao.getInstance().getGraphDB();
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

        try (Transaction tx = graphDb.beginTx()) {
            ResourceIterator<Node> aisTracks = graphDb.findNodes(Labels.ais_track, "rowkey", ais_rowkey);
            ArrayList<Node> aisNodes = new ArrayList<>();
            while (aisTracks.hasNext()) {
                aisNodes.add(aisTracks.next());
            }
            ResourceIterator<Node> radarTracks = graphDb.findNodes(Labels.radar_track, "rowkey", radar_rowkey);
            ArrayList<Node> radarNodes = new ArrayList<>();
            while (radarTracks.hasNext()) {
                radarNodes.add(radarTracks.next());
            }
            //1. ais键、雷达键都不存在
            if (aisNodes.isEmpty() && radarNodes.isEmpty()) {
                Node target = graphDb.createNode(Labels.target);
                Node aisTrack = graphDb.createNode(Labels.ais_track);
                aisTrack.setProperty("rowkey", ais_rowkey);
                Node radarTrack = graphDb.createNode(Labels.radar_track);
                radarTrack.setProperty("rowkey", radar_rowkey);
                Node property = graphDb.createNode(Labels.property);
                property.setProperty("mmsi", mmsi);
                Relationship r1 = target.createRelationshipTo(aisTrack, Relations.CONTAINS);
                r1.setProperty("start_time", ais_startTime);
                r1.setProperty("end_time", ais_endTime);
                Relationship r2 = target.createRelationshipTo(radarTrack, Relations.CONTAINS);
                r2.setProperty("start_time", radar_startTime);
                r2.setProperty("end_time", radar_endTime);
                Relationship r3 = target.createRelationshipTo(property, Relations.CONTAINS);
            }
            //2. ais键存在，雷达键不存在
            else if (!aisNodes.isEmpty() && radarNodes.isEmpty()) {

            }
            tx.success();
        }


        //3. ais键不存在，雷达键存在

        //4. ais键、雷达键都存在
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
