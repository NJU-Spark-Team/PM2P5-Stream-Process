import com.cloudera.sparkts.models.*;
import com.opencsv.CSVReader;
import org.apache.commons.math3.exception.MathIllegalStateException;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimulationExample {

    public static void main(String[] args) throws IOException {
        CSVReader reader = new CSVReader(new FileReader("/home/nosolution/Sundry/PM2.5 Data of Five Chinese Cities/Beijing_c.csv"));
        reader.readNext();
        List<String[]> list = reader.readAll();
        List<Double> pmRecords = new ArrayList<>();
        list.forEach(record -> {
            if (record != null && record.length > 0) {
                pmRecords.add(Double.parseDouble(record[4]));
            }

        });
        int startIdx = 0;
        int width = 4320;
        int predictN = 10;
        int step = 20;
        int count = 0;

        double[] ori = new double[width];
        double[] testset = new double[predictN];
        while (startIdx + width + predictN < pmRecords.size()) {
            for (int i = 0; i < width; i++)
                ori[i] = pmRecords.get(startIdx + i);
            Vector ts = Vectors.dense(ori);
            Vector result = hwForecast(ts, 10, 72);
            if (result == null)
                continue;
//            Vector result = arimaForcast(ts, 10);
            for (int i = 0; i < predictN; i++)
                testset[i] = pmRecords.get(startIdx + width + i);
            System.out.println("sse at " + count + " is: " + sse(testset, result.toArray()));
            double[] diffRatio = diff(testset, result.toArray());
            System.out.println("diff ratio is: ");
            for (int i = 0; i < predictN; i++)
                System.out.print(diffRatio[i] + " ");
            System.out.println("\n");


            count++;
            startIdx += step;
        }


    }


    /**
     * 使用Holt-Winter模型对时间序列进行拟合
     *
     * @param ts         time series, 按时间排列的属性值，本项目中为pm2.5
     * @param predictedN 预测值的个数
     * @param period     时间序列的周期(需要人工估计)
     * @return 按顺序排列的预测值
     */
    private static Vector hwForecast(Vector ts, int predictedN, int period) {

        try {
            HoltWintersModel model = HoltWinters.fitModelWithBOBYQA(ts, period, "additive");
            Vector dest = Vectors.zeros(predictedN);
            model.forecast(ts, dest);
            return dest;
        } catch (MathIllegalStateException e) {
            //存在计算失败的情况，需要考虑怎么填入无法预测的值
            e.printStackTrace();
            return null;
        }
    }

    /**
     * 使用ARIMA模型对时间序列进行拟合
     *
     * @param ts       time series, 按时间排列的属性值，本项目中为pm2.5
     * @param predictN 预测值的个数
     * @return 按顺序排列的预测值
     */
    private static Vector arimaForcast(Vector ts, int predictN) {
        ARIMAModel model = ARIMA.autoFit(ts, 10, 10, 10);
        return model.forecast(ts, predictN);
    }


    private static double sse(double[] o1, double[] o2) {
        int n = o1.length;
        double sse = 0;
        for (int i = 0; i < n; i++)
            sse += (o1[i] - o2[i]) * (o1[i] - o2[i]);
        sse /= (n - 1);
        return sse;
    }

    private static double[] diff(double[] o1, double[] o2) {
        int n = o1.length;
        double[] res = new double[n];
        for (int i = 0; i < n; i++)
            res[i] = Math.abs(o1[i] - o2[i]) / (o1[i] + o2[i]) * 2;
        return res;
    }
}
