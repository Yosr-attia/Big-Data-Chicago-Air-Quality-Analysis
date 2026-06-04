// NeighborhoodPollutionReducer.java
package tn.insat.bigdata.job3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class NeighborhoodPollutionReducer
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double sum = 0;
        int count  = 0;
        for (DoubleWritable val : values) {
            sum += val.get();
            count++;
        }
        if (count > 0)
            context.write(key, new DoubleWritable(sum / count));
    }
}