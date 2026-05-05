// PM25CleaningMapper.java
package tn.insat.bigdata.job1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class PM25CleaningMapper
        extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString().replace("\"", "");
        // Skip header
        if (line.startsWith("datasourceid")) return;

        String[] fields = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
        if (fields.length < 48) return;

        try {
            String sensorId   = fields[0].trim();
            String timestamp  = fields[2].trim();
            String sensorName = fields[3].trim();
            // pm2_5ConcMass1HourMean.value = index 24
            String pm25raw    = fields[23].trim().replace(",", ".");
            String pm25val    = fields[24].trim().replace(",", ".");
            String lat        = fields[45].trim().replace(",", ".");
            String lon        = fields[46].trim().replace(",", ".");

            if (pm25val.isEmpty()) return;
            Double.parseDouble(pm25val); // valide

            // Extraire quartier : supprimer numéro final
            String neighborhood = sensorName.replaceAll("\\s+\\d+$", "").trim();

            String outVal = timestamp + "," + pm25val + "," +
                    neighborhood + "," + lat + "," + lon;
            context.write(new Text(sensorId), new Text(outVal));

        } catch (Exception e) {
            // ligne invalide, on ignore
        }
    }
}