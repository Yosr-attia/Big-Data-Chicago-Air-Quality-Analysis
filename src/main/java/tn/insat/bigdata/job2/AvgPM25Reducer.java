// AvgPM25Reducer.java
package tn.insat.bigdata.job2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class AvgPM25Reducer
        extends Reducer<Text, DoubleWritable, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double sum = 0, min = Double.MAX_VALUE, max = Double.MIN_VALUE;
        int count = 0;

        for (DoubleWritable val : values) {
            double v = val.get();
            sum += v;
            if (v < min) min = v;
            if (v > max) max = v;
            count++;
        }

        double avg = sum / count;
        String result = String.format("avg=%.2f min=%.2f max=%.2f count=%d",
                avg, min, max, count);
        context.write(key, new Text(result));
    }
}