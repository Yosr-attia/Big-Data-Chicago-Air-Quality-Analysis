// HourlyPollutionReducer.java
package tn.insat.bigdata.job4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class HourlyPollutionReducer
        extends Reducer<Text, DoubleWritable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double sum = 0;
        int count  = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }
        double avg = sum / count;
        context.write(key,
                new Text(String.format("avg_pm25=%.2f  nb_mesures=%d", avg, count)));
    }
}