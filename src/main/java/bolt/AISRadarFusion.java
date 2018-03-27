package bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import entity.AISPlot;
import entity.RadarPlot;
import net.sf.json.JSONObject;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import util.Computation;
import util.HBaseDao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class AISRadarFusion extends BaseBasicBolt {
    private Connection conn;
    private Table RadarTracks;
    //todo:注意阈值的设定
    private static final double MINDISTANCE = 700;

    public void prepare(Map stormConf, TopologyContext context) {
        conn = HBaseDao.getInstance().getConnection();
        try {
            RadarTracks = conn.getTable(TableName.valueOf("RadarTracks"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        JSONObject jsonObject = (JSONObject) tuple.getValue(0);
        String ais_rowKey = jsonObject.getString("ais_rowKey");
        long ais_startTime = jsonObject.getLong("ais_startTime");
        long ais_endTime = jsonObject.getLong("ais_endTime");
        String mmsi = jsonObject.getString("mmsi");
        List<AISPlot> ais_track = (ArrayList<AISPlot>) jsonObject.get("ais_plots");

        long radar_startTime = 0;
        long radar_endTime = 0;
        double distance = MINDISTANCE;
        String radarRowkey_final = null;

        Scan scan = new Scan();
        ResultScanner scanner = null;
        try {
            scanner = RadarTracks.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (Result result : scanner) {
            String radar_rowKey = Bytes.toString(result.getRow());
            String radar_plots = Bytes.toString(result.getValue(Bytes.toBytes("value"), Bytes.toBytes("plots")));
            radar_startTime = Long.valueOf(radar_rowKey.substring(1, 12));
            radar_endTime = Long.valueOf(radar_rowKey.substring(13, 23));

            if (!(radar_endTime < ais_startTime || radar_startTime > ais_endTime)) {
                List<RadarPlot> radar_track = new ArrayList<RadarPlot>();
                for (String radar_plot : radar_plots.split(";")) {
                    String[] tmp = radar_plot.split(",");
                    RadarPlot radarPlot = new RadarPlot();
                    radarPlot.setLongitude(Double.valueOf(tmp[0]));
                    radarPlot.setLatitude(Double.valueOf(tmp[1]));
                    radarPlot.setTimestamp(Long.valueOf(tmp[2]));
                    radar_track.add(radarPlot);
                }
                double distance_tmp = Computation.computeDistanceOfTwoTracks(radar_track, ais_track);
                if (distance_tmp < distance) {
                    distance = distance_tmp;
                    radarRowkey_final = radar_rowKey;
                }
            }
        }

        //没有与雷达关联上
        if (radarRowkey_final == null) {
            //todo:添加是否与雷达关联上标志位？
        } else {
            JSONObject result = new JSONObject();
            result.put("ais_rowkey", ais_rowKey);
            result.put("radar_rowkey", radarRowkey_final);
            result.put("ais_startTime", ais_startTime);
            result.put("ais_endTime", ais_endTime);
            result.put("radar_startTime", radar_startTime);
            result.put("radar_endTime", radar_endTime);
            result.put("mmsi", mmsi);
            basicOutputCollector.emit(new Values(result));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("AISRadarFusion"));
    }
}
