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
	 * 查询指定数据库下的所有集合
	 */
	public static void getAllCollections() {
		MongoIterable<String> colls = mongoDatabase.listCollectionNames();
		for (String s : colls) {
			System.out.println(s);
		}
	}
	/**
	 * 创建集合
	 */
	public static void createCollection(String colname){
		mongoDatabase.createCollection(colname);
	}
	/**
	 * 获取集合
	 */
	public static MongoCollection<Document> getCollection(String colname){
		MongoCollection<Document> collection=mongoDatabase.getCollection(colname);
		return collection;
	}
	/**
	 * 删除集合
	 */
	public static void dropCollection(String colname){
		mongoDatabase.getCollection(colname).drop();
	}
	/** 
	 * 集合记录数(集合中的文档数)
	 */
	public static Long getCount(String colname) {
         long count = getCollection(colname).count();
         System.out.println("集合 "+colname+",记录数是="+count);
         return count;
	}
	/**
	 * 插入文档
	 */
	public static void insertMany(String colname,Document document){
		//获取集合
		MongoCollection<Document> collection=getCollection(colname);
		//构建文档对象
		List<Document> documents = new ArrayList<Document>(); 
		
		List<Map<String,Object>> addrAttrs = new ArrayList<Map<String,Object>>();
		for(int i=0;i<2;i++){
			Map<String,Object> map=new HashMap<String,Object>();
			map.put("privence","北京");
			map.put("city","海淀");
			addrAttrs.add(map);
		}
		int k=0;
		for(int i=0;i<300000;i++){
			Document doc = new Document("title", "MongoDB").  
			         append("description", "database").  
			         append("likes", 100+i).  
			         append("by", "fdz").
			         //append("address", new Document("privence","北京").append("city","海淀")); 
			         append("address", addrAttrs); 
	        documents.add(doc);  
	        //System.out.println("执行了.."+k+++"次");
		}
        collection.insertMany(documents); 
        System.out.println("执行了完毕");
	}
	/**
	 * 插入文档
	 */
	public static void insertOne(String colname,Document document){
		//获取集合
		MongoCollection<Document> collection=getCollection(colname);
		//构建文档对象
		Document doc = new Document("title", "MongoDB").  
		         append("description", "database").  
		         append("likes", 200).  
		         append("by", "fdz"); 
        collection.insertOne(doc); 
	}
	/**
	 * 删除文档
	 */
	public static void delDocument(String colname){
		//获取集合
		MongoCollection<Document> collection=getCollection(colname);
		//删除一条
		DeleteResult deleteResult=collection.deleteOne(Filters.eq("by","fdz"));
		//DeleteResult deleteResult=collection.deleteOne(Filters.eq("_id",new ObjectId("5a6003b34e8c6e1d78ffb9d9")));
		//删除多条
		//DeleteResult deleteResult=collection.deleteMany(Filters.eq("likes", 100));
		//DeleteResult deleteResult=collection.deleteMany(Filters.exists("by"));
		//DeleteResult deleteResult=collection.deleteMany(Filters.in("likes", Arrays.asList(100,200)));
		//DeleteResult deleteResult=collection.deleteMany(Filters.gte("likes",102));
		System.out.println("执行多条删除，成功删除了="+deleteResult.getDeletedCount()+"条");
	}
	
	/**
	 * 更新文档
	 */
	public static void updateDocument(String colname){
		//获取集合
		MongoCollection<Document> collection=getCollection(colname);
		//查询作者是fdz的记录，更新likes 与 description
		UpdateResult  updateResult =collection.updateMany(
				Filters.eq("by", "fdz"),
				new Document("$set",new Document("likes",200).append("description", "mongodatabase")));
		System.out.println("执行多条更新，成功更新了="+updateResult.getModifiedCount()+"条");
		
		//更新一条
		/*collection.updateOne(
				Filters.eq("by", "fdz"), 
				new Document("$set",new Document("likes",100).append("description", "database")));*/
		/*collection.updateOne(
				Filters.eq("_id", new ObjectId("5a6005364e8c6e1790c836c4")), 
				new Document("$set",new Document("likes",300).append("description", "db")));*/
		
	}
	
	/**
	 * 查询文档
	 */
	public static void findDocument(String colname){
		//获取集合
		MongoCollection<Document> collection=getCollection(colname);
        
		//1、指定ID查询
		//FindIterable<Document> findIterable = collection.find(Filters.eq("_id", new ObjectId("5a6005364e8c6e1790c836c4")));
		FindIterable<Document> findIterable = collection.find(Filters.eq("likes", "MongoDB"));
        
		//2、查询likes 在 100,200的数据
		//FindIterable<Document> findIterable = collection.find(Filters.in("likes",Arrays.asList(100,200))); 
		
		//3、查询description=database and likes>=100 
		/*Bson and = Filters.and(Filters.eq("by", "fdz"), Filters.gte("likes", 100));
		FindIterable<Document> findIterable = collection.find(and);*/
		
		//4、查询description=database or likes>=100
		//FindIterable<Document> findIterable = collection.find(Filters.or(Filters.eq("description", "database"), Filters.gte("likes", 100)));
		
		//5、查询description=database and likes>=100 使用运算符“$lt”,"$gt","$lte","$gte"
		/*Document query1 = new Document("description", "database").append("likes", new Document("$gte",100));
		FindIterable<Document> findIterable=collection.find(query1);*/
		//第二种方式也可以如下
		/*Document query2 = new Document("description", "database");
		query2.put("likes", new Document("$gte",100));
		FindIterable<Document> findIterable=collection.find(query2);*/
		
		//6、查询description=database or likes>=100
		/*Document query3 = new Document("$or", Arrays.asList(new Document("description", "database"), new Document("likes", new Document("$gte",100))));		
		FindIterable<Document> findIterable=collection.find(query3);*/
		
		//7、查询 likes  between 100 and 200
		/*Document query4 =new Document("likes", new Document("$gte",100).append("$lte", 200));		
		FindIterable<Document> findIterable=collection.find(query4);*/
		
		/*MongoCursor<Document> mongoCursor = findIterable.iterator();  
        while(mongoCursor.hasNext()){ 
           String jsonString=mongoCursor.next().toJson();
           System.out.println("jsonString==="+jsonString);
        }*/
		
		//遍历文档的另一种方法  
		long s=System.currentTimeMillis();
        for (Document doc : findIterable) {  
        	System.out.println("原文档="+doc);
            System.out.println("转json="+doc.toJson());  
        }
        long e=System.currentTimeMillis();
        System.out.println("总共用时="+(e-s)/1000f+"秒");
	}
	/**
	 * 排序
	 */
	public static void findDocumentSort(String colname){
		MongoCollection<Document> collection=getCollection(colname);
		
		//按照likes升序排序
		Bson asc_sort=Sorts.ascending("likes");
		//按照likes降序排序
		Bson desc_sort=Sorts.descending("likes");
		//按照likes升序排序，_id降序
		Bson asc_desc_sort=Sorts.orderBy(Sorts.ascending("likes"), Sorts.descending("_id"));
		
		FindIterable<Document> findIterable = collection.find(Filters.or(Filters.eq("description", "database"), Filters.gte("likes", 100))).sort(asc_desc_sort);
        for (Document cur : findIterable) {  
            System.out.println(cur.toJson());  
        }
	}
	
	/**
	 * 查询只显示部分字段
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
	 * 分页查询
	 */
	public static MongoCursor<Document> findByPage(String colname, Bson filter, int pageNo, int pageSize) {
		//获取集合
		MongoCollection<Document> collection=getCollection(colname);
		//id升序排序
		Bson orderBy = new BasicDBObject("_id", 1);
		MongoCursor<Document> mongoCursor=collection.find(filter)
				.sort(orderBy).skip((pageNo - 1) * pageSize).limit(pageSize).iterator();
		while(mongoCursor.hasNext()){ 
	           String jsonString=mongoCursor.next().toJson();
	           System.out.println("分页jsonString==="+jsonString);
	    }
		return mongoCursor;
	}
	/**
	 * 其他的分组、统计方法自己研究总结
	 */
	
	public static void main(String args[]) {
		//dropCollection("mycol");
		//MongoTest.createCollection("mycol");
		//MongoUtil.getAllDBNames();
		//insertOne("mycol",null);
		insertMany("mycol",null);
		//delDocument("mycol");
		//updateDocument("mycol");
		//findDocument("mycol");//0.524秒  0.552秒
		//findDocumentSort("mycol");
		//findSubDocument("mycol");
		//getCount("mycol");
		//findByPage("mycol",Filters.exists("by"),2,2);
		//getAllCollections();
		//关闭连接
		MongoUtil.closeMongoClient();
	}

}
