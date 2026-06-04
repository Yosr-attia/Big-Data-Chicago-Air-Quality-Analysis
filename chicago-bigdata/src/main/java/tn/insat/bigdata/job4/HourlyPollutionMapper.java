// HourlyPollutionMapper.java
package tn.insat.bigdata.job4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class HourlyPollutionMapper
        extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] parts = value.toString().split("\t");
        if (parts.length < 2) return;

        String[] vals = parts[1].split(",");
        if (vals.length < 2) return;

        try {
            // timestamp format: "02/08/2026 07:00:00 PM"
            String ts    = vals[0].trim();
            String[] tsp = ts.split(" ");
            if (tsp.length < 3) return;

            String[] timeParts = tsp[1].split(":");
            int hour = Integer.parseInt(timeParts[0]);
            String ampm = tsp[2];
            if (ampm.equals("PM") && hour != 12) hour += 12;
            if (ampm.equals("AM") && hour == 12) hour  = 0;

            double pm25 = Double.parseDouble(vals[1].trim());
            context.write(new Text(String.format("%02d", hour)),
                    new DoubleWritable(pm25));
        } catch (Exception e) { }
    }
}