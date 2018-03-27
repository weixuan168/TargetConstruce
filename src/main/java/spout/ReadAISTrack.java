package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import entity.AISPlot;
import net.sf.json.JSONObject;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import util.HBaseDao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ReadAISTrack extends BaseRichSpout {

    private Connection conn;
    private Table AISTracks;
    private SpoutOutputCollector collector;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        conn = HBaseDao.getInstance().getConnection();
        try {
            AISTracks = conn.getTable(TableName.valueOf("AISTracks"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        collector = spoutOutputCollector;
    }

    public void close() {
        try {
            conn.close();
            AISTracks.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void nextTuple() {
        //没有被读过的行记录，没有isRead列；被读过的行记录，isRead列值为true
        //todo:被读过的行记录，但没有关联上某种手段，如何处理
        Scan scan = new Scan();
        SingleColumnValueFilter scvf = new SingleColumnValueFilter(Bytes.toBytes("value"),
                Bytes.toBytes("isRead"), CompareFilter.CompareOp.NOT_EQUAL, Bytes.toBytes("true"));
        scvf.setFilterIfMissing(false);//如果该列本身就不存在，则包括该行。
        scan.setFilter(scvf);
        ResultScanner scanner = null;
        try {
            scanner = AISTracks.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (Result result : scanner) {
            String rowKey = Bytes.toString(result.getRow());
            String mmsi = Bytes.toString(result.getValue(Bytes.toBytes("value"), Bytes.toBytes("mmsi")));
            String plots = Bytes.toString(result.getValue(Bytes.toBytes("value"), Bytes.toBytes("plots")));

            //todo:注意实际中用的是11位还是13位
            long ais_startTime = Long.valueOf(rowKey.substring(1, 12));
            long ais_endTime = Long.valueOf(rowKey.substring(13, 23));
            List<AISPlot> ais_track = new ArrayList<AISPlot>();
            for (String ais_plot : plots.split(";")) {
                String[] tmp = ais_plot.split(",");
                AISPlot aisPlot = new AISPlot();
                //todo:注意实际存值顺序与此对应
                aisPlot.setLongitude(Double.valueOf(tmp[0]));
                aisPlot.setLatitude(Double.valueOf(tmp[1]));
                aisPlot.setDirection(Double.valueOf(tmp[2]));
                aisPlot.setHeading(Integer.valueOf(tmp[3]));
                aisPlot.setSpeed(Double.valueOf(tmp[4]));
                aisPlot.setTimestamp(Long.valueOf(tmp[5]));
                ais_track.add(aisPlot);
            }
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("ais_rowKey", rowKey);
            jsonObject.put("ais_startTime", ais_startTime);
            jsonObject.put("ais_endTime", ais_endTime);
            jsonObject.put("mmsi", mmsi);
            jsonObject.put("ais_plots",ais_track);
            collector.emit(new Values(jsonObject));
        }
        try {
            //todo:spout间隔多长时间读一次
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ReadAISTrack"));
    }
}
