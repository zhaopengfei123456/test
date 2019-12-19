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

	//������̬����  
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
	//(����һ)�жϱ��Ƿ����,������ھ�ɾ�� 
	private static void isExistDel(String tableName) throws Exception  {
		//hbase �������
		admin =(HBaseAdmin) conn.getAdmin();
		if(admin.tableExists(tableName))   {    
			admin.disableTable(tableName);     
			admin.deleteTable(tableName);
		}
	}
	//�����������������ݱ�  
	public static void createTable(String tableName,String[] columnFamilys) throws Exception  {   
		//����ǰ�淽��һ��ɾ���Ѿ����ڵ�tableName��   
		isExistDel(tableName);   
		//�½�һ���������   
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(tableName));   
		for (String strFamily : columnFamilys) {
			//�����дض���
			HColumnDescriptor Colfamily = new HColumnDescriptor(strFamily.getBytes()); 
			//����д�
			desc.addFamily(Colfamily);   
		}   
		//��ӵ�������  
		admin.createTable(desc);  
		System.out.println("������Ķ��� "+tableName+" �ɹ���");   
	}
	//(������)���һ������   
	public static void addRow(String tableName,String row, String columnFamily,String column,String value)throws Exception  {   
		//��ȡ�����  
		Table table = conn.getTable(TableName.valueOf(tableName));
		putData = new Put(row.getBytes());   
		putData.addColumn(columnFamily.getBytes(), column==null?null:column.getBytes(), value.getBytes());   
		table.put(putData);   
		table.close(); 
	}
	//(������)��ȡһ��(��)����
	public static void getRow(String tableName,String row) throws Exception  {   
		//��ȡ�����  
		Table table = conn.getTable(TableName.valueOf(tableName)); 
		Get getRow = new Get(row.getBytes());   
		Result rt =  table.get(getRow);   
		int n=1;
		/* ��ǰ�ķ��������Ƽ�ʹ��
		KeyValue[] kv = rt.raw();
		for (KeyValue keyValue : kv) {    
			System.out.println();
			//����    
			System.out.print(n+" �м���"+Bytes.toString(keyValue.getRow()));    
			System.out.print("�дأ�"+Bytes.toString(keyValue.getFamily())+"  ");    
			System.out.print("�У�"+Bytes.toString(keyValue.getQualifier())+"  ");    
			System.out.print("ʱ�����"+keyValue.getTimestamp()+"  ");
			//ʱ�����long����    
			System.out.print("��ֵ��"+new String(keyValue.getValue())+"  ");    
			n++;   
		} */
		List<Cell> cells=rt.listCells();
		getItem(n,cells);
		//����   
		System.out.println();  
		table.close();
	}
	//(������)��ȡһ��ָ�����塢�е�����
	public static void getRow(String tableName,String row,String columnFamily,String column) throws Exception  { 
		System.out.println("----ָ���е�����---"); 
		//��ȡ�����  
		Table table = conn.getTable(TableName.valueOf(tableName)); 
		Get getRow = new Get(row.getBytes()); 
		//ָ������
		getRow.addFamily(Bytes.toBytes(columnFamily)); 
		//ָ����
		getRow.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
		Result rt =  table.get(getRow);   
		int n=1;
		
		List<Cell> cells=rt.listCells();
		getItem(n,cells);
		//����   
		System.out.println();  
		table.close();
	}
	//(������) ��ȡ�������ȫ������   
	public static void getAllRows(String tableName)throws Exception  {
		System.out.println("---��ñ��ȫ��������---"); 
		Table table = conn.getTable(TableName.valueOf(tableName)); 
		// Scan ȫ��ɨ�����
		Scan scan = new Scan();
		// ֻ��ѯ����info
		scan.addFamily(Bytes.toBytes("address")); 
		// ֻ��ѯ��name
		scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name")); 
		//�õ�ɨ����   
		ResultScanner rs = table.getScanner(scan);   
		int n=1;
		//��ʾ�кš�û����������   
		for (Result result : rs) {    
			List<Cell> cells=result.listCells();
			getItem(n,cells);   
			System.out.println();  
		}  
		//����   
		System.out.println();
		table.close();  
	}
	//(������) ��ֵ���ˣ�������ֵ����ȡ����ȡ���Χ�ȣ�ȡ����������  
	public static void getRowsFilter(String tableName)throws Exception  {
		System.out.println("---��ֵ���� ȡ����������---"); 
		Table table = conn.getTable(TableName.valueOf(tableName)); 
		/**
	     * CompareOp ��һ��ö�٣������¼���ֵ
	     * LESS                 С��
	     * LESS_OR_EQUAL        С�ڻ����
	     * EQUAL                ����
	     * NOT_EQUAL            ������
	     * GREATER_OR_EQUAL     ���ڻ����
	     * GREATER              ����
	     * NO_OP                �޲���
	     */
		SingleColumnValueFilter columnValueFilter = new SingleColumnValueFilter(
				 Bytes.toBytes("info"), Bytes.toBytes("age"), 
				 CompareOp.GREATER_OR_EQUAL,  Bytes.toBytes("31"));
		// Scan ȫ��ɨ�����
		Scan scan = new Scan();
		// ֻ��ѯ����info
		//scan.addFamily(Bytes.toBytes("info")); 
		// ֻ��ѯ��name
		//scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
		scan.setFilter(columnValueFilter);
		//ɨ����   
		ResultScanner rs = table.getScanner(scan);
		int n=1;
		//��ʾ�кš�û����������   
		for (Result result : rs) {    
			List<Cell> cells=result.listCells();
			getItem(n,cells);
			System.out.println();  
		}  
		//����   
		System.out.println();
		table.close();  
	}
	//(������) ����ǰ׺������������ָ��ǰ׺��������ȡ����������  
	public static void getRowsPrefixFilter(String tableName)throws Exception  {
		System.out.println("---����ǰ׺������ ȡ����������---"); 
		Table table = conn.getTable(TableName.valueOf(tableName)); 
		// ��ѯ��������name��ͷ������
		ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("name"));
		//�������ǰ׺�����������˶��ָ��ǰ׺��������
		byte[][] bytes = new byte[][]{Bytes.toBytes("name"), Bytes.toBytes("age")};
        MultipleColumnPrefixFilter multipleColumnPrefixFilter = new MultipleColumnPrefixFilter(bytes);
        //rowKey��������ͨ�����򣬹���rowKeyֵ��
        // ƥ��rowkey��100��ͷ������
        //Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("^100"));
        // ƥ��rowkey��2��β������
        //RowFilter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("2$"));
		// Scan ȫ��ɨ�����
		Scan scan = new Scan();
		// ֻ��ѯ����info
		//scan.addFamily(Bytes.toBytes("info")); 
		// ֻ��ѯ��name
		//scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));
		scan.setFilter(multipleColumnPrefixFilter);
		//ɨ����   
		ResultScanner rs = table.getScanner(scan);   
		int n=1;
		for (Result result : rs) {    
			List<Cell> cells=result.listCells();
			getItem(n,cells);
			System.out.println();  
		}  
		//����   
		System.out.println();
		table.close();  
	}
	//(������)ɾ�����ݱ�   
	public static void deleteTale(String tableName)throws Exception  {    
		isExistDel(tableName);   
		System.out.print("ɾ���ɹ���");  
	}   
	//(������)ɾ��һ������   
	public static void deleteRow(String tableName,String row)throws Exception  {   
		Table table = conn.getTable(TableName.valueOf(tableName));    
		Delete delRow = new Delete(row.getBytes());   
		table.delete(delRow);   
		System.out.print("ɾ�� "+row+" �гɹ���");   
		table.close();  
	}
	//�������ˣ�ɾ����������   
	public static void deleteMultiRows(String tableName,String[] rows)throws Exception  {    
		Table table = conn.getTable(TableName.valueOf(tableName)); 
	    //����һ��List����   
		List<Delete> delList = new ArrayList<Delete>();   
		for (String row : rows) {    
			Delete del = new Delete(row.getBytes());    
			delList.add(del);
			//��ӵ�list����   
		}   
		//����table��������ķ���ɾ��   
		table.delete(delList);  
	}
	//ͨ��ѭ�����ϵķ���
	private static void getItem(int n,List<Cell> cells){
		for(Cell cell:cells){
			System.out.println();
			System.out.print(n+" �м���"+Bytes.toString(CellUtil.cloneRow(cell)));    
			System.out.print("�дأ�"+Bytes.toString(CellUtil.cloneFamily(cell))+"  ");    
			System.out.print("�У�"+Bytes.toString(CellUtil.cloneQualifier(cell))+"  ");    
			System.out.print("ʱ�����"+cell.getTimestamp()+"  ");
			//ʱ�����long����    
			System.out.print("��ֵ��"+new String(CellUtil.cloneValue(cell))+"  "); 
			n++;
		} 
	}
	public static void main(String[] args) throws Exception {

		System.out.println("---------------------------------------------"); 
		//����   
		String tableName = "emply";
		//�д�
		String[] columnFamilys = new String[]{"info","address"};
		//1.ɾ���Ѿ����ڵ�ͬ���ı�   
		//isExistDel(tableName);   
		//2.������   
		createTable(tableName, columnFamilys);   

		//3.�������   
		addRow(tableName, "000", "info", "name", "jack");
		addRow(tableName, "000", "info", "age", "30");
		addRow(tableName, "000", "info", "sex", "��");
		addRow(tableName, "000", "address", "provence", "china");
		addRow(tableName, "000", "address", "city", "beijin");
		addRow(tableName, "000", "address", "county", "haidian");
		
		addRow(tableName, "001", "info", "name", "tom");
		addRow(tableName, "001", "info", "age", "20");
		addRow(tableName, "001", "info", "sex", "women");
		addRow(tableName, "001", "address", "provence", "china");
		addRow(tableName, "001", "address", "city", "shanghai");
		addRow(tableName, "001", "address", "county", "hongqiao");
		//4.��ȡһ������     
		//getRow(tableName, "000"); 
        //5.ָ���е�ֵ
		//getRow(tableName, "000","info","name"); 
		//6.��ȡ��������   
		//getAllRows(tableName); 
		//7.����ֵ��ѯ
		//getRowsFilter(tableName);
		//8.��ֵǰ׺��ѯ
		//getRowsPrefixFilter(tableName);
		//9.ɾ��һ������     
		//deleteRow(tableName, "000");     
		//10.ɾ����������     
		deleteMultiRows(tableName, new String[]{"000","001"}); 
		if(admin!=null){
			admin.close();
		}
	}

}
