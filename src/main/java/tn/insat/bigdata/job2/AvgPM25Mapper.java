// AvgPM25Mapper.java
package tn.insat.bigdata.job2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class AvgPM25Mapper
        extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Input depuis Job1: sensorId \t timestamp,pm25,neighborhood,lat,lon
        String[] parts = value.toString().split("\t");
        if (parts.length < 2) return;

        String sensorId = parts[0].trim();
        String[] vals   = parts[1].split(",");
        if (vals.length < 2) return;

        try {
            // timestamp: "02/08/2026 07:00:00 PM"
            String ts   = vals[0].trim();
            String date = ts.split(" ")[0]; // "02/08/2026"
            double pm25 = Double.parseDouble(vals[1].trim());

            String compositeKey = sensorId + "#" + date;
            context.write(new Text(compositeKey), new DoubleWritable(pm25));
        } catch (Exception e) { }
    }
}