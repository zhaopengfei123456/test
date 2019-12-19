package com.bigdata.mongo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;

public class MongoTest {

	private static MongoDatabase mongoDatabase;
	
	static{
		
		mongoDatabase=MongoUtil.getDatabase("newdb");
	}

	/**
	 * ��ѯָ�����ݿ��µ����м���
	 */
	public static void getAllCollections() {
		MongoIterable<String> colls = mongoDatabase.listCollectionNames();
		for (String s : colls) {
			System.out.println(s);
		}
	}
	/**
	 * ��������
	 */
	public static void createCollection(String colname){
		mongoDatabase.createCollection(colname);
	}
	/**
	 * ��ȡ����
	 */
	public static MongoCollection<Document> getCollection(String colname){
		MongoCollection<Document> collection=mongoDatabase.getCollection(colname);
		return collection;
	}
	/**
	 * ɾ������
	 */
	public static void dropCollection(String colname){
		mongoDatabase.getCollection(colname).drop();
	}
	/** 
	 * ���ϼ�¼��(�����е��ĵ���)
	 */
	public static Long getCount(String colname) {
         long count = getCollection(colname).count();
         System.out.println("���� "+colname+",��¼����="+count);
         return count;
	}
	/**
	 * �����ĵ�
	 */
	public static void insertMany(String colname,Document document){
		//��ȡ����
		MongoCollection<Document> collection=getCollection(colname);
		//�����ĵ�����
		List<Document> documents = new ArrayList<Document>(); 
		
		List<Map<String,Object>> addrAttrs = new ArrayList<Map<String,Object>>();
		for(int i=0;i<2;i++){
			Map<String,Object> map=new HashMap<String,Object>();
			map.put("privence","����");
			map.put("city","����");
			addrAttrs.add(map);
		}
		int k=0;
		for(int i=0;i<300000;i++){
			Document doc = new Document("title", "MongoDB").  
			         append("description", "database").  
			         append("likes", 100+i).  
			         append("by", "fdz").
			         //append("address", new Document("privence","����").append("city","����")); 
			         append("address", addrAttrs); 
	        documents.add(doc);  
	        //System.out.println("ִ����.."+k+++"��");
		}
        collection.insertMany(documents); 
        System.out.println("ִ�������");
	}
	/**
	 * �����ĵ�
	 */
	public static void insertOne(String colname,Document document){
		//��ȡ����
		MongoCollection<Document> collection=getCollection(colname);
		//�����ĵ�����
		Document doc = new Document("title", "MongoDB").  
		         append("description", "database").  
		         append("likes", 200).  
		         append("by", "fdz"); 
        collection.insertOne(doc); 
	}
	/**
	 * ɾ���ĵ�
	 */
	public static void delDocument(String colname){
		//��ȡ����
		MongoCollection<Document> collection=getCollection(colname);
		//ɾ��һ��
		DeleteResult deleteResult=collection.deleteOne(Filters.eq("by","fdz"));
		//DeleteResult deleteResult=collection.deleteOne(Filters.eq("_id",new ObjectId("5a6003b34e8c6e1d78ffb9d9")));
		//ɾ������
		//DeleteResult deleteResult=collection.deleteMany(Filters.eq("likes", 100));
		//DeleteResult deleteResult=collection.deleteMany(Filters.exists("by"));
		//DeleteResult deleteResult=collection.deleteMany(Filters.in("likes", Arrays.asList(100,200)));
		//DeleteResult deleteResult=collection.deleteMany(Filters.gte("likes",102));
		System.out.println("ִ�ж���ɾ�����ɹ�ɾ����="+deleteResult.getDeletedCount()+"��");
	}
	
	/**
	 * �����ĵ�
	 */
	public static void updateDocument(String colname){
		//��ȡ����
		MongoCollection<Document> collection=getCollection(colname);
		//��ѯ������fdz�ļ�¼������likes �� description
		UpdateResult  updateResult =collection.updateMany(
				Filters.eq("by", "fdz"),
				new Document("$set",new Document("likes",200).append("description", "mongodatabase")));
		System.out.println("ִ�ж������£��ɹ�������="+updateResult.getModifiedCount()+"��");
		
		//����һ��
		/*collection.updateOne(
				Filters.eq("by", "fdz"), 
				new Document("$set",new Document("likes",100).append("description", "database")));*/
		/*collection.updateOne(
				Filters.eq("_id", new ObjectId("5a6005364e8c6e1790c836c4")), 
				new Document("$set",new Document("likes",300).append("description", "db")));*/
		
	}
	
	/**
	 * ��ѯ�ĵ�
	 */
	public static void findDocument(String colname){
		//��ȡ����
		MongoCollection<Document> collection=getCollection(colname);
        
		//1��ָ��ID��ѯ
		//FindIterable<Document> findIterable = collection.find(Filters.eq("_id", new ObjectId("5a6005364e8c6e1790c836c4")));
		FindIterable<Document> findIterable = collection.find(Filters.eq("likes", "MongoDB"));
        
		//2����ѯlikes �� 100,200������
		//FindIterable<Document> findIterable = collection.find(Filters.in("likes",Arrays.asList(100,200))); 
		
		//3����ѯdescription=database and likes>=100 
		/*Bson and = Filters.and(Filters.eq("by", "fdz"), Filters.gte("likes", 100));
		FindIterable<Document> findIterable = collection.find(and);*/
		
		//4����ѯdescription=database or likes>=100
		//FindIterable<Document> findIterable = collection.find(Filters.or(Filters.eq("description", "database"), Filters.gte("likes", 100)));
		
		//5����ѯdescription=database and likes>=100 ʹ���������$lt��,"$gt","$lte","$gte"
		/*Document query1 = new Document("description", "database").append("likes", new Document("$gte",100));
		FindIterable<Document> findIterable=collection.find(query1);*/
		//�ڶ��ַ�ʽҲ��������
		/*Document query2 = new Document("description", "database");
		query2.put("likes", new Document("$gte",100));
		FindIterable<Document> findIterable=collection.find(query2);*/
		
		//6����ѯdescription=database or likes>=100
		/*Document query3 = new Document("$or", Arrays.asList(new Document("description", "database"), new Document("likes", new Document("$gte",100))));		
		FindIterable<Document> findIterable=collection.find(query3);*/
		
		//7����ѯ likes  between 100 and 200
		/*Document query4 =new Document("likes", new Document("$gte",100).append("$lte", 200));		
		FindIterable<Document> findIterable=collection.find(query4);*/
		
		/*MongoCursor<Document> mongoCursor = findIterable.iterator();  
        while(mongoCursor.hasNext()){ 
           String jsonString=mongoCursor.next().toJson();
           System.out.println("jsonString==="+jsonString);
        }*/
		
		//�����ĵ�����һ�ַ���  
		long s=System.currentTimeMillis();
        for (Document doc : findIterable) {  
        	System.out.println("ԭ�ĵ�="+doc);
            System.out.println("תjson="+doc.toJson());  
        }
        long e=System.currentTimeMillis();
        System.out.println("�ܹ���ʱ="+(e-s)/1000f+"��");
	}
	/**
	 * ����
	 */
	public static void findDocumentSort(String colname){
		MongoCollection<Document> collection=getCollection(colname);
		
		//����likes��������
		Bson asc_sort=Sorts.ascending("likes");
		//����likes��������
		Bson desc_sort=Sorts.descending("likes");
		//����likes��������_id����
		Bson asc_desc_sort=Sorts.orderBy(Sorts.ascending("likes"), Sorts.descending("_id"));
		
		FindIterable<Document> findIterable = collection.find(Filters.or(Filters.eq("description", "database"), Filters.gte("likes", 100))).sort(asc_desc_sort);
        for (Document cur : findIterable) {  
            System.out.println(cur.toJson());  
        }
	}
	
	/**
	 * ��ѯֻ��ʾ�����ֶ�
	 */
	public static void findSubDocument(String colname){
		MongoCollection<Document> collection=getCollection(colname);
		
		Document query =new Document("likes", new Document("$gt",100).append("$lt", 300));		
		FindIterable<Document> findIterable=collection.find(query).projection(new BasicDBObject().append("_id", 0).append("title", 1));
        for (Document cur : findIterable) {  
            System.out.println(cur.toJson());  
        }
	}
	/** 
	 * ��ҳ��ѯ
	 */
	public static MongoCursor<Document> findByPage(String colname, Bson filter, int pageNo, int pageSize) {
		//��ȡ����
		MongoCollection<Document> collection=getCollection(colname);
		//id��������
		Bson orderBy = new BasicDBObject("_id", 1);
		MongoCursor<Document> mongoCursor=collection.find(filter)
				.sort(orderBy).skip((pageNo - 1) * pageSize).limit(pageSize).iterator();
		while(mongoCursor.hasNext()){ 
	           String jsonString=mongoCursor.next().toJson();
	           System.out.println("��ҳjsonString==="+jsonString);
	    }
		return mongoCursor;
	}
	/**
	 * �����ķ��顢ͳ�Ʒ����Լ��о��ܽ�
	 */
	
	public static void main(String args[]) {
		//dropCollection("mycol");
		//MongoTest.createCollection("mycol");
		//MongoUtil.getAllDBNames();
		//insertOne("mycol",null);
		insertMany("mycol",null);
		//delDocument("mycol");
		//updateDocument("mycol");
		//findDocument("mycol");//0.524��  0.552��
		//findDocumentSort("mycol");
		//findSubDocument("mycol");
		//getCount("mycol");
		//findByPage("mycol",Filters.exists("by"),2,2);
		//getAllCollections();
		//�ر�����
		MongoUtil.closeMongoClient();
	}

}
