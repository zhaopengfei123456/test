package com.bigdata.mongo;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

public class MongoUtil {
	
	private static final String IP="127.0.0.1"; 
	
	private static final int PROT=27017; 
	
	private static MongoClient mongoClient;
	
	private static MongoDatabase mongoDatabase;
	
	static {
		// 连接到 mongodb 服务
		mongoClient = new MongoClient(IP, PROT);
	}
	/**
	 * 列出所有数据库名称
	 */
	public static MongoIterable<String> getAllDBNames() {
		MongoIterable<String> databases = mongoClient.listDatabaseNames();
		//遍历表名称
		MongoCursor<String> it=databases.iterator();
		while(it.hasNext()){
			System.out.println(it.next().toString());
		}
		return databases;
    }
	/**
	 * 连接指定数据库
	 */
	public static MongoDatabase getDatabase(String dbname){
		try {
			// 连接到数据库
			mongoDatabase = mongoClient.getDatabase(dbname);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return mongoDatabase;
	}
    /**
     * 删除指定的数据库
     */
	public static void dropDatabase(String dbname){
		try {
			// 连接到数据库
			mongoClient.dropDatabase(dbname);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
     * 关闭数据库连接
     */
	public static void closeMongoClient(){
		if(mongoClient!=null){
			mongoClient.close();
		}
	}
	
	public static void main(String[] args) {
		getAllDBNames();
	}
}
