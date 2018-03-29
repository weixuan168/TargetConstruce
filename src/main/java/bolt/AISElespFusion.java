package bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import entity.AISPlot;
import entity.ElespPlot;
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

public class AISElespFusion extends BaseBasicBolt {
    private Connection conn;
    private Table ElespTracks;
    //todo:注意阈值的设定
    private static final double MINDISTANCE = 700;

    public void prepare(Map stormConf, TopologyContext context) {
        conn = HBaseDao.getInstance().getConnection();
        try {
            ElespTracks = conn.getTable(TableName.valueOf("ElespTracks"));
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

        long elesp_startTime = 0;
        long elesp_endTime = 0;

        double mindistance = MINDISTANCE;
        List<String> elespRowkey_finals = new ArrayList<String>();
        List<String> sfc_finals = new ArrayList<String>();
        List<Double> distances = new ArrayList<Double>();
        List<String> rowkey_tmp = new ArrayList<String>();
        List<String> sfcs = new ArrayList<String>();

        Scan scan = new Scan();
        ResultScanner scanner = null;
        try {
            scanner = ElespTracks.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (Result result : scanner) {
            String elesp_rowKey = Bytes.toString(result.getRow());
            String elesp_plots = Bytes.toString(result.getValue(Bytes.toBytes("value"), Bytes.toBytes("plots")));
            elesp_startTime = Long.valueOf(elesp_rowKey.substring(1, 11));
            elesp_endTime = Long.valueOf(elesp_rowKey.substring(13, 22));
            //todo:要不要转int
            String sfc = Bytes.toString(result.getValue(Bytes.toBytes("value"), Bytes.toBytes("sfc")));

            if (!(elesp_endTime < ais_startTime || elesp_startTime > ais_endTime)) {
                List<ElespPlot> elesp_track = new ArrayList<ElespPlot>();
                for (String elesp_plot : elesp_plots.split(";")) {
                    String[] tmp = elesp_plot.split(",");
                    ElespPlot elespPlot = new ElespPlot();
                    elespPlot.setLongitude(Double.valueOf(tmp[0]));
                    elespPlot.setLatitude(Double.valueOf(tmp[1]));
                    elespPlot.setTimestamp(Long.valueOf(tmp[2]));
                    elesp_track.add(elespPlot);
                }
                double distance_tmp = Computation.computeDistanceOfTwoTracks(elesp_track, ais_track);
                //记录所有比较的距离，电磁轨迹行键，中心频率
                if (distance_tmp < MINDISTANCE) {
                    distances.add(distance_tmp);
                    rowkey_tmp.add(elesp_rowKey);
                    sfcs.add(sfc);
                    //记录最近的一个距离
                    if (distance_tmp < mindistance) {
                        mindistance = distance_tmp;
                    }
                }
            }
        }

        //todo:找distance距离20米（？）以内的轨迹
        for (int i = 0; i < distances.size(); i++) {
            if (distances.get(i) - mindistance < 20) {
                elespRowkey_finals.add(rowkey_tmp.get(i));
                sfc_finals.add(sfcs.get(i));
            }
        }

        if (elespRowkey_finals.isEmpty()) {
            //todo:添加是否与电磁关联上标志位？
        } else {
            JSONObject result = new JSONObject();
            result.put("ais_rowkey", ais_rowKey);
            result.put("elesp_rowkey", elespRowkey_finals);
            result.put("ais_startTime", ais_startTime);
            result.put("ais_endTime", ais_endTime);
            result.put("elesp_startTime", elesp_startTime);
            result.put("elesp_endTime", elesp_endTime);
            result.put("mmsi", mmsi);
            result.put("sfc", sfc_finals);
            basicOutputCollector.emit(new Values(result));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("AISElespFusion"));
    }
}
