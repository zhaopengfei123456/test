package com.bigdata.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class Hbase {

	//声明静态配置  
	private static Configuration conf = null;  
	private static HBaseAdmin admin=null; 
	private static Connection conn = null;
	private static Put putData = null;
	private static String IP="192.168.226.128";
	static{
		try {
			System.setProperty("hadoop.home.dir", "E:\\worksoft\\hadoop\\hadoop-2.4.1");
			conf = HBaseConfiguration.create();   
			conf.set("hbase.rootdir", "hdfs://"+IP+"/hbase");   
			conf.set("hbase.zookeeper.quorum",IP+":2181");
			conn = ConnectionFactory.createConnection(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	//(方法一)判断表是否存在,如果存在就删除 
	private static void isExistDel(String tableName) throws Exception  {
		//hbase 表管理类
		admin =(HBaseAdmin) conn.getAdmin();
		if(admin.tableExists(tableName))   {    
			admin.disableTable(tableName);     
			admin.deleteTable(tableName);
		}
	}
	//（方法二）创建数据表  
	public static void createTable(String tableName,String[] columnFamilys) throws Exception  {   
		//调用前面方法一，删除已经存在的tableName表   
		isExistDel(tableName);   
		//新建一个表的描述   
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));   
		for (String strFamily : columnFamilys) {
			//构建列簇对象
			HColumnDescriptor Colfamily = new HColumnDescriptor(strFamily.getBytes()); 
			//添加列簇
			desc.addFamily(Colfamily);   
		}   
		//添加到表里面  
		admin.createTable(desc);  
		System.out.println("创建表的定义 "+tableName+" 成功！");   
	}
	//(方法三)添加一条数据   
	public static void addRow(String tableName,String row, String columnFamily,String column,String value)throws Exception  {   
		//获取表对象  
		Table table = conn.getTable(TableName.valueOf(tableName));
		putData = new Put(row.getBytes());   
		putData.addColumn(columnFamily.getBytes(), column==null?null:column.getBytes(), value.getBytes());   
		table.put(putData);   
		table.close(); 
	}
	//(方法四)获取一条(行)数据
	public static void getRow(String tableName,String row) throws Exception  {   
		//获取表对象  
		Table table = conn.getTable(TableName.valueOf(tableName)); 
		Get getRow = new Get(row.getBytes());   
		Result rt =  table.get(getRow);   
		int n=1;
		/* 以前的方法，不推荐使用
		KeyValue[] kv = rt.raw();
		for (KeyValue keyValue : kv) {    
			System.out.println();
			//换行    
			System.out.print(n+" 行键："+Bytes.toString(keyValue.getRow()));    
			System.out.print("列簇："+Bytes.toString(keyValue.getFamily())+"  ");    
			System.out.print("列："+Bytes.toString(keyValue.getQualifier())+"  ");    
			System.out.print("时间戳："+keyValue.getTimestamp()+"  ");
			//时间戳是long类型    
			System.out.print("列值："+new String(keyValue.getValue())+"  ");    
			n++;   
		} */
		List<Cell> cells=rt.listCells();
		getItem(n,cells);
		//换行   
		System.out.println();  
		table.close();
	}
	//(方法五)获取一条指定列族、列的数据
	public static void getRow(String tableName,String row,String columnFamily,String column) throws Exception  { 
		System.out.println("----指定列的数据---"); 
		//获取表对象  
		Table table = conn.getTable(TableName.valueOf(tableName)); 
		Get getRow = new Get(row.getBytes()); 
		//指定列族
		getRow.addFamily(Bytes.toBytes(columnFamily)); 
		//指定列
		getRow.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
		Result rt =  table.get(getRow);   
		int n=1;
		
		List<Cell> cells=rt.listCells();
		getItem(n,cells);
		//换行   
		System.out.println();  
		table.close();
	}
	//(方法六) 获取表里面的全部数据   
	public static void getAllRows(String tableName)throws Exception  {
		System.out.println("---获得表的全部的数据---"); 
		Table table = conn.getTable(TableName.valueOf(tableName)); 
		// Scan 全表扫描对象
		Scan scan = new Scan();
		// 只查询列族info
		scan.addFamily(Bytes.toBytes("address")); 
		// 只查询列name
		scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name")); 
		//得到扫描器   
		ResultScanner rs = table.getScanner(scan);   
		int n=1;
		//显示行号。没有其他作用   
		for (Result result : rs) {    
			List<Cell> cells=result.listCells();
			getItem(n,cells);   
			System.out.println();  
		}  
		//换行   
		System.out.println();
		table.close();  
	}
	//(方法七) 列值过滤（过滤列值的相等、不等、范围等）取表里面数据  
	public static void getRowsFilter(String tableName)throws Exception  {
		System.out.println("---列值过滤 取表里面数据---"); 
		Table table = conn.getTable(TableName.valueOf(tableName)); 
		/**
	     * CompareOp 是一个枚举，有如下几个值
	     * LESS                 小于
	     * LESS_OR_EQUAL        小于或等于
	     * EQUAL                等于
	     * NOT_EQUAL            不等于
	     * GREATER_OR_EQUAL     大于或等于
	     * GREATER              大于
	     * NO_OP                无操作
	     */
		SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(
				 Bytes.toBytes("info"), Bytes.toBytes("age"), 
				 CompareOp.GREATER_OR_EQUAL,  Bytes.toBytes("31"));
		// Scan 全表扫描对象
		Scan scan = new Scan();
		// 只查询列族info
		//scan.addFamily(Bytes.toBytes("info")); 
		// 只查询列name
		//scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
		scan.setFilter(columnValueFilter);
		//扫描器   
		ResultScanner rs = table.getScanner(scan);
		int n=1;
		//显示行号。没有其他作用   
		for (Result result : rs) {    
			List<Cell> cells=result.listCells();
			getItem(n,cells);
			System.out.println();  
		}  
		//换行   
		System.out.println();
		table.close();  
	}
	//(方法八) 列名前缀过滤器（过滤指定前缀的列名）取表里面数据  
	public static void getRowsPrefixFilter(String tableName)throws Exception  {
		System.out.println("---列名前缀过滤器 取表里面数据---"); 
		Table table = conn.getTable(TableName.valueOf(tableName)); 
		// 查询列名称是name开头的数据
		ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("name"));
		//多个列名前缀过滤器（过滤多个指定前缀的列名）
		byte[][] bytes = new byte[][]{Bytes.toBytes("name"), Bytes.toBytes("age")};
        MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(bytes);
        //rowKey过滤器（通过正则，过滤rowKey值）
        // 匹配rowkey以100开头的数据
        //Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^100"));
        // 匹配rowkey以2结尾的数据
        //RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("2$"));
		// Scan 全表扫描对象
		Scan scan = new Scan();
		// 只查询列族info
		//scan.addFamily(Bytes.toBytes("info")); 
		// 只查询列name
		//scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
		scan.setFilter(multipleColumnPrefixFilter);
		//扫描器   
		ResultScanner rs = table.getScanner(scan);   
		int n=1;
		for (Result result : rs) {    
			List<Cell> cells=result.listCells();
			getItem(n,cells);
			System.out.println();  
		}  
		//换行   
		System.out.println();
		table.close();  
	}
	//(方法六)删除数据表   
	public static void deleteTale(String tableName)throws Exception  {    
		isExistDel(tableName);   
		System.out.print("删除成功！");  
	}   
	//(方法七)删除一行数据   
	public static void deleteRow(String tableName,String row)throws Exception  {   
		Table table = conn.getTable(TableName.valueOf(tableName));    
		Delete delRow = new Delete(row.getBytes());   
		table.delete(delRow);   
		System.out.print("删除 "+row+" 行成功！");   
		table.close();  
	}
	//（方法八）删除多行数据   
	public static void deleteMultiRows(String tableName,String[] rows)throws Exception  {    
		Table table = conn.getTable(TableName.valueOf(tableName)); 
	    //创建一个List集合   
		List<Delete> delList = new ArrayList<Delete>();   
		for (String row : rows) {    
			Delete del = new Delete(row.getBytes());    
			delList.add(del);
			//添加到list里面   
		}   
		//利用table对象里面的方法删除   
		table.delete(delList);  
	}
	//通用循环集合的方法
	private static void getItem(int n,List<Cell> cells){
		for(Cell cell:cells){
			System.out.println();
			System.out.print(n+" 行键："+Bytes.toString(CellUtil.cloneRow(cell)));    
			System.out.print("列簇："+Bytes.toString(CellUtil.cloneFamily(cell))+"  ");    
			System.out.print("列："+Bytes.toString(CellUtil.cloneQualifier(cell))+"  ");    
			System.out.print("时间戳："+cell.getTimestamp()+"  ");
			//时间戳是long类型    
			System.out.print("列值："+new String(CellUtil.cloneValue(cell))+"  "); 
			n++;
		} 
	}
	public static void main(String[] args) throws Exception {

		System.out.println("---------------------------------------------"); 
		//表名   
		String tableName = "emply";
		//列簇
		String[] columnFamilys = new String[]{"info","address"};
		//1.删除已经存在的同名的表   
		//isExistDel(tableName);   
		//2.创建表   
		createTable(tableName, columnFamilys);   

		//3.添加数据   
		addRow(tableName, "000", "info", "name", "jack");
		addRow(tableName, "000", "info", "age", "30");
		addRow(tableName, "000", "info", "sex", "男");
		addRow(tableName, "000", "address", "provence", "china");
		addRow(tableName, "000", "address", "city", "beijin");
		addRow(tableName, "000", "address", "county", "haidian");
		
		addRow(tableName, "001", "info", "name", "tom");
		addRow(tableName, "001", "info", "age", "20");
		addRow(tableName, "001", "info", "sex", "women");
		addRow(tableName, "001", "address", "provence", "china");
		addRow(tableName, "001", "address", "city", "shanghai");
		addRow(tableName, "001", "address", "county", "hongqiao");
		//4.获取一条数据     
		//getRow(tableName, "000"); 
        //5.指定列的值
		//getRow(tableName, "000","info","name"); 
		//6.获取所有数据   
		//getAllRows(tableName); 
		//7.过滤值查询
		//getRowsFilter(tableName);
		//8.列值前缀查询
		//getRowsPrefixFilter(tableName);
		//9.删除一行数据     
		//deleteRow(tableName, "000");     
		//10.删除多条数据     
		deleteMultiRows(tableName, new String[]{"000","001"}); 
		if(admin!=null){
			admin.close();
		}
	}

}
