package entity;

/**
 * 电磁点实体类
 */
public class ElespPlot {
    private double longitude;
    private double latitude;
    private int sfc;    //中心频率
    private long timestamp;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public int getSfc() {
        return sfc;
    }

    public void setSfc(int sfc) {
        this.sfc = sfc;
    }
}
