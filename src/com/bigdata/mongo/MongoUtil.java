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
		// ���ӵ� mongodb ����
		mongoClient = new MongoClient(IP, PROT);
	}
	/**
	 * �г��������ݿ�����
	 */
	public static MongoIterable<String> getAllDBNames() {
		MongoIterable<String> databases = mongoClient.listDatabaseNames();
		//����������
		MongoCursor<String> it=databases.iterator();
		while(it.hasNext()){
			System.out.println(it.next().toString());
		}
		return databases;
    }
	/**
	 * ����ָ�����ݿ�
	 */
	public static MongoDatabase getDatabase(String dbname){
		try {
			// ���ӵ����ݿ�
			mongoDatabase = mongoClient.getDatabase(dbname);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return mongoDatabase;
	}
    /**
     * ɾ��ָ�������ݿ�
     */
	public static void dropDatabase(String dbname){
		try {
			// ���ӵ����ݿ�
			mongoClient.dropDatabase(dbname);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
     * �ر����ݿ�����
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
