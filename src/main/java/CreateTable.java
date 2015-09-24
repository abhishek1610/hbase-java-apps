import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Put;

public class CreateTable {
    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        conf.addResource("hbase-site.xml")
        conf.set("hbase.zookeeper.quorum","quickstart.cloudera")
        conf.set("hbase.zookeeper.property.clientPort","2181")
        conf.set("hbase.master", "quickstart.cloudera:60010")
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("my-table"));
        tableDescriptor.addFamily(new HColumnDescriptor("colfam1"));
        tableDescriptor.addFamily(new HColumnDescriptor("colfam2"));
        tableDescriptor.addFamily(new HColumnDescriptor("colfam3"));
        admin.createTable(tableDescriptor);
        boolean tableAvailable = admin.isTableAvailable("my-table");
        System.out.println("tableAvailable = " + tableAvailable);

        HTable hTable = new HTable(conf, "my-table");

        Put p = new Put(Bytes.toBytes("row1"));

        p.add(Bytes.toBytes("colfam1"),
                Bytes.toBytes("col1"),Bytes.toBytes("abhi"));

        hTable.put(p);
        System.out.println("data inserted");

        // closing HTable
        hTable.close();
    }
}