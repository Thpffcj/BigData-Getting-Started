package cn.edu.nju.domain;

/**
 * Created by Thpffcj on 2018/4/10.
 */
public class ResultBean {

    private double lng;
    private double lat;
    private long count;

    public double getLng() {
        return lng;
    }

    public void setLng(double lng) {
        this.lng = lng;
    }

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "ResultBean{" +
                "lng=" + lng +
                ", lat=" + lat +
                ", count=" + count +
                '}';
    }
}
