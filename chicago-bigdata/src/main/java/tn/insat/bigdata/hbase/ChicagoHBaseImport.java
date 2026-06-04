// ChicagoHBaseImport.java
package tn.insat.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.*;
import java.nio.file.*;

public class ChicagoHBaseImport {

    public static void main(String[] args) throws Exception {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "hadoop-master");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        try (Connection conn = ConnectionFactory.createConnection(config);
             Admin admin = conn.getAdmin()) {

            TableName tableName = TableName.valueOf("chicago_air_quality");

            // Recréer la table si elle existe
            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
            TableDescriptor td = TableDescriptorBuilder
                    .newBuilder(tableName)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of("measures"))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of("meta"))
                    .build();
            admin.createTable(td);
            System.out.println("Table créée.");

            Table table = conn.getTable(tableName);
            BufferedReader reader = new BufferedReader(
                    new FileReader("chicago_air_quality.csv"));

            String line;
            boolean header = true;
            int count = 0;

            while ((line = reader.readLine()) != null) {
                if (header) { header = false; continue; }
                String clean = line.replace("\"", "");
                String[] f   = clean.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
                if (f.length < 48) continue;

                try {
                    String sensorId  = f[0].trim();
                    String timestamp = f[2].trim(); // "02/08/2026 07:00:00 PM"
                    String sensorName= f[3].trim();
                    String pm25val   = f[24].trim().replace(",", ".");
                    String pm25nc    = f[26].trim().replace(",", ".");
                    String no2val    = f[18].trim().replace(",", ".");
                    String tempval   = f[38].trim().replace(",", ".");
                    String humidity  = f[34].trim().replace(",", ".");
                    String windspeed = f[42].trim().replace(",", ".");
                    String lat       = f[45].trim().replace(",", ".");
                    String lon       = f[46].trim().replace(",", ".");
                    String neighborhood = sensorName.replaceAll("\\s+\\d+$", "").trim();

                    if (pm25val.isEmpty()) continue;

                    // Row key: SENSOR_ID#YYYYMMDD#HH
                    String[] tsp    = timestamp.split(" ");
                    String[] datep  = tsp[0].split("/");
                    String dateStr  = datep[2] + datep[0] + datep[1]; // YYYYMMDD
                    String[] timep  = tsp[1].split(":");
                    int hour = Integer.parseInt(timep[0]);
                    if (tsp.length > 2 && tsp[2].equals("PM") && hour != 12) hour += 12;
                    if (tsp.length > 2 && tsp[2].equals("AM") && hour == 12) hour  = 0;
                    String rowKey = sensorId + "#" + dateStr + "#" +
                            String.format("%02d", hour);

                    Put put = new Put(Bytes.toBytes(rowKey));

                    if (!pm25val.isEmpty())
                        put.addColumn(Bytes.toBytes("measures"), Bytes.toBytes("pm25"),
                                Bytes.toBytes(pm25val));
                    if (!pm25nc.isEmpty())
                        put.addColumn(Bytes.toBytes("measures"), Bytes.toBytes("pm25_nowcast"),
                                Bytes.toBytes(pm25nc));
                    if (!no2val.isEmpty())
                        put.addColumn(Bytes.toBytes("measures"), Bytes.toBytes("no2"),
                                Bytes.toBytes(no2val));
                    if (!tempval.isEmpty())
                        put.addColumn(Bytes.toBytes("measures"), Bytes.toBytes("temp"),
                                Bytes.toBytes(tempval));
                    if (!humidity.isEmpty())
                        put.addColumn(Bytes.toBytes("measures"), Bytes.toBytes("humidity"),
                                Bytes.toBytes(humidity));
                    if (!windspeed.isEmpty())
                        put.addColumn(Bytes.toBytes("measures"), Bytes.toBytes("wind_speed"),
                                Bytes.toBytes(windspeed));

                    put.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("neighborhood"),
                            Bytes.toBytes(neighborhood));
                    put.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("lat"),
                            Bytes.toBytes(lat));
                    put.addColumn(Bytes.toBytes("meta"), Bytes.toBytes("lon"),
                            Bytes.toBytes(lon));

                    table.put(put);
                    count++;
                    if (count % 100 == 0)
                        System.out.println(count + " lignes importées...");

                } catch (Exception e) { /* ligne invalide */ }
            }
            reader.close();
            table.close();
            System.out.println("Import terminé : " + count + " enregistrements.");
        }
    }
}
