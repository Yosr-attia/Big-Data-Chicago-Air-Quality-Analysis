// NeighborhoodPollutionMapper.java
package tn.insat.bigdata.job3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class NeighborhoodPollutionMapper
        extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] parts = value.toString().split("\t");
        if (parts.length < 2) return;

        String[] vals = parts[1].split(",");
        if (vals.length < 3) return;

        try {
            double pm25         = Double.parseDouble(vals[1].trim());
            String neighborhood = vals[2].trim(); // index 2 = neighborhood
            if (!neighborhood.isEmpty())
                context.write(new Text(neighborhood), new DoubleWritable(pm25));
        } catch (Exception e) { }
    }
}