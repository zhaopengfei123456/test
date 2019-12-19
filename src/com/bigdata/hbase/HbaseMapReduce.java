package com.bigdata.hbase;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
public class HbaseMapReduce {

	//声明静态配置  
	private static Configuration conf = null;  
	private static HBaseAdmin admin=null; 
	private static Connection conn = null;
	private static Put putData = null;
	private static String IP="192.168.226.128";
	static{
		try {
			System.setProperty("hadoop.home.dir", "E:\\worksoft\\hadoop\\hadoop-2.4.1\\");
			conf = HBaseConfiguration.create();   
			conf.set("hbase.rootdir", "hdfs://"+IP+"/hbase");   
			conf.set("hbase.zookeeper.quorum",IP+":2181");
			conn = ConnectionFactory.createConnection(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * putDataInHbase:创建一个t_word表，插入数据 <br/>
	 * @author fdz
	 * @param tableName
	 * @throws Exception
	 */
	private static void putDataInHbase(String tableName,String[] columnFamilys) throws Exception  {
		//hbase 表管理类
		admin =(HBaseAdmin) conn.getAdmin();
		if(admin.tableExists(tableName))   {    
			admin.disableTable(tableName);     
			admin.deleteTable(tableName);
		}
		//创建表结构
		Hbase.createTable(tableName, columnFamilys);
		//插入数据
		Hbase.addRow(tableName, "000", columnFamilys[0], null, "The Apache Hadoop software library is a framework");
		Hbase.addRow(tableName, "001", columnFamilys[0], null, "The common utilities that support the other Hadoop modules");
		Hbase.addRow(tableName, "002", columnFamilys[0], null, "Hadoop by reading the documentation");
		Hbase.addRow(tableName, "003", columnFamilys[0], null, "Hadoop from the release page");
		Hbase.addRow(tableName, "004", columnFamilys[0], null, "Hadoop on the mailing list");
	}

	public static class MyMapper extends TableMapper<Text, IntWritable> {
        //IntWritable相当于java中Integer整型变量，为这个变量赋值为1
		private static IntWritable one = new IntWritable(1);
		private static Text word = new Text();
		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			// 表里面只有一个列族，所以我就直接获取每一行的值
			String words = Bytes.toString(value.list().get(0).getValue());
			StringTokenizer st = new StringTokenizer(words);
			while (st.hasMoreTokens()) {
				String s = st.nextToken();
				word.set(s);
				context.write(word, one);
			}
		}
	}
	//reduce阶段读取有多少个 相同的key  相加  得到一个总数
	public static class MyReduce extends TableReducer<Text, IntWritable, NullWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		    int sum = 0;
		    for (IntWritable i : values) {
		        sum += i.get();
		    }
		    Put put = new Put(Bytes.toBytes(key.toString()));
		    // Put实例化，每一个词存一行
		    put.add(Bytes.toBytes("content"),null,
		            Bytes.toBytes(String.valueOf(sum)));
		    // 列族为content，列值为数目
		    context.write(NullWritable.get(), put);
		}
    }
	public static void main(String[] args) throws Exception {

		System.out.println("---------------------------------------------"); 
		//列簇
		String[] columnFamilys = new String[]{"content"};
		putDataInHbase("t_word",columnFamilys);
		
		//Job job = new Job(conf,"t_word_count"); 
		Job job = Job.getInstance(conf, "t_word_count");
        job.setJarByClass(HbaseMapReduce.class);  
        Scan scan = new Scan();
        //scan.addFamily("count".getBytes());
        //创建统计结果写入的表结构
        Hbase.createTable("data_output",columnFamilys);
        
        TableMapReduceUtil.initTableMapperJob("t_word",scan, MyMapper.class, Text.class, IntWritable.class, job);
        //reduce阶段读取有多少个 相同的key  相加  得到一个总数, 将该总数存入data_output表中
        TableMapReduceUtil.initTableReducerJob("data_output", MyReduce.class, job);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
        //直接输出信息
        Table table = conn.getTable(TableName.valueOf("data_output")); 
		// Scan 全表扫描对象
		Scan sc = new Scan();
		ResultScanner rs = table.getScanner(sc);   
		Result r; 
		while (((r = rs.next()) != null)) {  
	            byte[] key = r.getRow();  
	            String kyes = Bytes.toString(key);  
	            byte[] totalValue = r.getValue(Bytes.toBytes("content"), null);  
	            String count = Bytes.toString(totalValue);  
	            System.out.println("key: " + kyes+ ",  count: " + count);  
	    }  
		table.close(); 
		
		if(admin!=null){
			admin.close();
		}
	}

}
