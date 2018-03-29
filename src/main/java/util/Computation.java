package util;

import entity.AISPlot;
import entity.RadarPlot;
import org.apache.commons.math3.analysis.polynomials.PolynomialFunction;
import org.apache.commons.math3.fitting.PolynomialCurveFitter;
import org.apache.commons.math3.fitting.WeightedObservedPoints;

import java.util.List;

public class Computation {

    static final double MAXDISTANCEOFTRACKS = 700;
    static final int FUSIONYZ = 10;

    //计算两条轨迹之间的距离
    //todo:要修改
    public static double computeDistanceOfTwoTracks(List<RadarPlot> radarTrack, List<AISPlot> aisTrack) {
        int fusionNumber = 0;
        double fusionSum = 0;
        Long MinTime = aisTrack.get(0).getTimestamp();
        Long MaxTime = aisTrack.get(aisTrack.size()-1).getTimestamp();
        WeightedObservedPoints obsx = new WeightedObservedPoints();
        WeightedObservedPoints obsy = new WeightedObservedPoints();
        int n = aisTrack.size();
        double sumqz = 0.0;
        Long[] time = new Long[n];
        double[] xs = new double[n];
        double[] ys = new double[n];
        for(int i=0;i<n;i++)
        {
            sumqz = sumqz+1/(Math.abs(aisTrack.get(i).getSpeed())+0.1);
        }

        for(int i=0;i<n;i++)
        {
            time[i] = aisTrack.get(i).getTimestamp();
            xs[i] = aisTrack.get(i).getLongitude();
            ys[i] = aisTrack.get(i).getLatitude();

            obsx.add(1/(Math.abs(aisTrack.get(i).getSpeed())+0.1)/sumqz,time[i],xs[i]);
            obsy.add(1/(Math.abs(aisTrack.get(i).getSpeed())+0.1)/sumqz,time[i],ys[i]);

        }
        PolynomialCurveFitter fitterx = PolynomialCurveFitter.create(2);
        double[] resx = fitterx.fit(obsx.toList());
        PolynomialFunction pfx = new PolynomialFunction(resx);

        PolynomialCurveFitter fittery = PolynomialCurveFitter.create(2);
        double[] resy = fittery.fit(obsy.toList());
        PolynomialFunction pfy = new PolynomialFunction(resy);

        for(int i=0;i<radarTrack.size();i++)
        {
            RadarPlot radarPlot = radarTrack.get(i);
            Long radarTime = radarPlot.getTimestamp();
            if(radarTime <= MaxTime && radarTime >= MinTime)
            {
                double[] res = {pfx.value(radarTime),pfy.value(radarTime)};
                double a = res[1]*Math.PI/180.0 - radarPlot.getLatitude()*Math.PI/180.0;
                double b = res[0]*Math.PI/180.0 - radarPlot.getLongitude()*Math.PI/180.0;
                double s = 2 * Math.asin(
                        Math.sqrt(
                                Math.pow(Math.sin(a/2),2)
                                        + Math.cos(res[1]*Math.PI/180.0)*Math.cos(radarPlot.getLatitude()*Math.PI/180.0)*Math.pow(Math.sin(b/2),2)
                        )
                );
                s = s * 6371000.0;
                s = Math.round(s * 10000) / 10000;
                fusionNumber++;
                fusionSum+=s;
            }
        }
        if(fusionNumber<FUSIONYZ)
        {
            return MAXDISTANCEOFTRACKS;
        }else

        {
            return fusionSum/(fusionNumber*fusionNumber);
        }
    }
}
