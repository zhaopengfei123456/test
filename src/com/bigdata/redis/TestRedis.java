package com.bigdata.redis;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class TestRedis {
	/*
	 * 替换数字正则 \d{1,}[ ]{4}
	 */
	private Jedis jedis;

	@Before
	public void setup() {
		// 连接redis服务器，192.168.0.100:6379
		// jedis = new Jedis("127.0.0.1", 6379);
		// 权限认证
		//jedis.auth("admin");
		jedis = RedisUtil.getJedis();
	}
	/**
	 * 操作数据库
	 */
	@Test
	public void testdb() {
		//设置数据库，默认就是0
		jedis.select(0);
		//通过通配符获取所有key
		Set<String> keys=jedis.keys("*");
		Iterator<String> it=keys.iterator();
		System.out.println("当前数据库"+jedis.getDB()+",获取所有key,数量="+jedis.dbSize());
		int k=0;
		while(it.hasNext()){
			String key=it.next();
			k++;
			System.out.println(k+" key 名称="+key+"类型是="+jedis.type(key));
		}
		//判断指定的key是否存在
		System.out.println("是否存在名称为name的key="+jedis.exists("name"));
		//删除指定的key
		long n=jedis.del("name","key1");
		System.out.println("删除了"+n+"个key");
		//返回当前数据库中的随机key
		System.out.println("当前数据库中的随机key="+jedis.randomKey());
		//重命名一个key,如果newkey存在，值将会被覆盖，返回1表示成功，0失败
		jedis.mset("key1","000","key2","001");
		System.out.println("key1的值是="+jedis.get("key1")+",key2的值是="+jedis.get("key2"));
		jedis.rename("key1", "key2");//将key1重命名为key2,key1即不存在，key2的值是key1原来的值
		System.out.println("重命名后，key1的值是="+jedis.get("key1")+",key2的值是="+jedis.get("key2"));
		//为key指定过期时间，单位是秒
		jedis.expire("key2", 30);
		System.out.println("key2离过期还剩  "+jedis.ttl("key2")+"秒");
		//删除当前数据库中所有key
		System.out.println("删除当前数据库中所有key="+jedis.flushDB());
	    //删除所有数据库中的所有key
		System.out.println("删除所有数据库中的所有key="+jedis.flushAll());
	}
    /**
    * redis存储字符串
    */
	@Test
	public void testString() {
		// -----添加数据----------
		jedis.set("name", "冯德智");
		// 如果key已经存在，返回0
		long k = jedis.setnx("name", "马云");
		System.out.println(jedis.get("name") + "k==" + k);
		// getset key value 原子的设置key的值，并返回key的旧值。如果key不存在返回nil
		String v1 = jedis.getSet("name", "刘备");
		System.out.println("jedis.get(name)=" + jedis.get("name") + ", getset的置是==" + v1);
		// 拼接
		jedis.append("name", " is my lover");
		System.out.println("拼接后的值是=" + jedis.get("name"));
		// 删除某个键
		jedis.del("name");
		System.out.println("删除键name后的值是=" + jedis.get("name"));
		// 设置多个键值对
		jedis.mset("name", "冯德智", "age", "35", "qq", "476777XXX");
		// 进行加1操作
		jedis.incr("age");
		// 进行减1操作
		jedis.decr("age");
		// 给age加指定值
		jedis.incrBy("age", 10);
		// 给age减指定值
		jedis.decrBy("age", 15);
		System.out.println("name键对应的value长度=" + jedis.strlen("name"));
		// 截取key的字符串值,并不修改key的值。下标是从0开始的
		System.out.println("截取后的值是" + jedis.substr("name", 0, 2));
		// 获取多个键的值
		List<String> lists = jedis.mget("name", "age", "qq");
		System.out.println("----多个键值输出---------");
		for (String value : lists) {
			System.out.println(value);
		}
	}
   
	/**
	 * redis操作Map
	 */
	@Test
	public void testMap() {
		// 单值添加
		jedis.hset("userinfo", "name", "冯德智");
		// 单值取值
		System.out.println("取单个值=" + jedis.hget("userinfo", "name"));
		// 添加多个值
		Map<String, String> map = new HashMap<String, String>();
		map.put("name", "xinxin");
		map.put("age", "22");
		map.put("qq", "123456");
		jedis.hmset("user", map);
		// 将指定的hash filed 加上给定值
		jedis.hincrBy("user", "age", 14);
		// 获取多个值
		List<String> rsmap = jedis.hmget("user", "name", "age", "qq");
		System.out.println("获取多个值=" + rsmap);
		// 删除map中的某个键值
		jedis.hdel("user", "age");
		// 测试指定field是否存在
		System.out.println("测试指定age元素是否存在" + jedis.hexists("user", "age"));
		// 返回key为user的键中存放的值的个数
		System.out.println("user的键中存放的值元素个数=" + jedis.hlen("user"));
		// 是否存在key为user的记录
		System.out.println("是否存在key为user的记录=" + jedis.exists("user"));
		// 返回map对象中的所有key
		System.out.println("返回map对象中的所有key=" + jedis.hkeys("user"));
		// 返回map对象中的所有value
		System.out.println("返回map对象中的所有value=" + jedis.hvals("user"));
		// 返回指定key的所有filed和value
		Map<String, String> maps = jedis.hgetAll("user");
		Set<String> keys = maps.keySet();
		for (String key : keys) {
			System.out.println("key=" + key + ",value=" + maps.get(key));
		}
	}
   
	/**
	 * jedis操作List
	 */
	@Test
	public void testList() {
		// 先向键 javaframework中,从头部添加字符串元素 存放三条数据
		jedis.lpush("javaframework", "spring");
		jedis.lpush("javaframework", "struts");
		jedis.lpush("javaframework", "hibernate");
		// 再向键 javaframework中,从尾部添加字符串元素 存放三条数据
		jedis.rpush("javaframework", "mybitis");
		jedis.rpush("javaframework", "springMVC");
		jedis.rpush("javaframework", "springboot");
		// 获取元素个数
		System.out.println("list键javaframework中元素个数是=" + jedis.llen("javaframework"));
		// 截取list，保留指定区间内元素，成功返回1，key不存在返回错误,包前也包后
		jedis.ltrim("javaframework", 0, 6);
		// lset key index value 设置list中指定下标的元素值，成功返回1，key或者下标不存在返回错误
		jedis.lset("javaframework", 0, "springclood");
		// 从key对应list中删除count个和value相同的元素。count为0时候删除全部
		jedis.lrem("javaframework", 1, "springclood");
		// lpop key 从list的头部删除元素，并返回删除元素
		jedis.lpop("javaframework");
		// rpop key 从list的尾部删除元素，并返回删除元素
		jedis.rpop("javaframework");
				
		/**
		 * blpop timeout key1...keyN 从左到右扫描返回对第一个非空list进行lpop操作并返回， 比如blpop 0
		 * list1 list2 list3
		 * ,如果list1不存在list2,list3都是非空,则对list2做lpop并返回从list2中删除的元素。
		 * 如果所有的list都是空或不存在，则会阻塞timeout秒，timeout为0表示一直阻塞,
		 * 当阻塞时，如果有client对key1...keyN中的任意key进行push操作,则第一在这个key上被阻塞的client会立即返回。
		 * 如果超时发生，则返回nil 
		 * brpop 同blpop，一个是从头部删除一个是从尾部删除
		 */
		jedis.blpop(2, "javaframework");
		// 遍历list中的所有value
		List<String> lists = jedis.lrange("javaframework", 0, jedis.llen("javaframework"));
		int k = 0;
		System.out.println("----遍历list----");
		for (String value : lists) {
			k++;
			System.out.println(k + " list中各个元素的值是=" + value);
		}
		// 删除javaframework 键
		jedis.del("javaframework");
	}  
	/**
	 * jedis操作Set
	 */
	@Test
	public void testSet() {
		// 添加
		jedis.sadd("sset1", "刘备");
		jedis.sadd("sset1", "关羽");
		jedis.sadd("sset1", "张飞");
		jedis.sadd("sset1", "貂蝉");
		jedis.sadd("sset1", "赵云");
		jedis.sadd("sset1", "000");
		jedis.sadd("sset2", "000");
		jedis.sadd("sset2", "001");
		jedis.sadd("sset3", "a");
		jedis.sadd("sset3", "b");
		// 移除貂蝉
		jedis.srem("sset1", "貂蝉");
		// 删除并返回key对应set中随机的一个元素
		System.out.println("删除并返回key对应set中随机的一个元素=" + jedis.spop("sset1"));
		// 随机取set中的一个元素，但是不删除元素
		System.out.println("随机取set中的一个元素=" + jedis.srandmember("sset1"));
		// 从sset1中移除 元素并添加到sset2对应set中
		jedis.smove("sset1", "sset2", "刘备");
		// 判断元素是否在set中，存在返回1,否则返回0
		System.out.println("sset2中是否包含刘备=" + jedis.sismember("sset2", "刘备"));
		// 获取元素个数
		System.out.println("sset1元素个数是=" + jedis.scard("sset1") + ",sset2元素个数是=" + jedis.scard("sset2"));
		// 返回所有给定key的交集
		System.out.println("-------输出交集------");
		Set<String> sinners = jedis.sinter("sset1", "sset2");
		Iterator<String> innerit = sinners.iterator();
		while (innerit.hasNext()) {
			System.out.println("交集的元素是=" + innerit.next());
		}
		// 返回所有给定key的交集,同时将交集存到sset3下
		System.out.println("-------输出保存的交集------");
		jedis.sinterstore("sset3", "sset1", "sset2");
		System.out.println("sset3中所有元素是=" + jedis.smembers("sset3"));
		// 返回所有给定key的并集
		System.out.println("-------输出并集------");
		Set<String> sunions = jedis.sunion("sset1", "sset2");
		for (String value : sunions) {
			System.out.println("并集的元素是=" + value);
		}
		// 返回并集，且保存到指定的sset3集合
		System.out.println("-------输出保存的并集------");
		jedis.sunionstore("sset3", "sset1", "sset2");
		System.out.println("sset3中所有元素是=" + jedis.smembers("sset3"));
		// 返回所有给定key的差集
		System.out.println("-------输出差集------");
		Set<String> diffs = jedis.sdiff("sset1", "sset2");
		for (String difvalue : diffs) {
			System.out.println("差集的元素是=" + difvalue);
		}
		// 返回差集，且保存到指定的sset3集合
		System.out.println("-------输出保存的差集------");
		jedis.sdiffstore("sset3", "sset1", "sset2");
		System.out.println("sset3中所有元素是=" + jedis.smembers("sset3"));
	}  
	/**
	 * jedis操作SortedSet
	 */
	@Test
	public void testSortedSet() {
		Map<Double, String> map=new HashMap<Double, String>();
		map.put(1.0,"java");
		map.put(2.0,"c#");
		map.put(3.0,"c++");
		map.put(-1.0,"python");
		//添加
		jedis.zadd("zset",map);
		//删除
		jedis.zrem("zset", "python");
		//增加元素的score值，返回更新后的score值
		jedis.zincrby("zset", 4, "java");
		//返回指定元素在集合中的排名,集合中元素是按score从小到大排序的
		System.out.println("java在集合中从小到大的排名="+jedis.zrank("zset", "java"));
		//返回指定元素在集合中的排名,集合中元素是按score从大到小排序的
		System.out.println("java在集合中从大道小的排名="+jedis.zrevrank("zset", "java"));
		//zrange正序遍历、zrevrange 逆序遍历
		System.out.println("按照score正序遍历结果="+jedis.zrange("zset", 0, 10));
		System.out.println("按照score逆序遍历结果="+jedis.zrevrange("zset", 0, 10));
		//返回集合中score在给定区间的元素
		System.out.println("返回集合中score在给定区间的元素遍历结果="+jedis.zrangeByScore("zset", "3", "4"));
		//返回集合中score在给定区间的数量
		System.out.println("集合中score在给定区间的数量结果="+jedis.zcount("zset", "3", "4"));
		//返回集合中元素个数
		System.out.println("集合中元素个数是="+jedis.zcard("zset"));
		//返回给定元素对应的score
		System.out.println("给定元素java对应的score值是="+jedis.zscore("zset", "java"));
		//删除集合中排名在给定区间的元素
		System.out.println("删除集合中排名在给定区间的元素结果是="+jedis.zremrangeByRank("zset", 0,1));
		//删除集合中score在给定区间的元素
		System.out.println("删除集合中score在给定区间的元素结果是="+jedis.zremrangeByScore("zset", 0,1));
	}
	/**
	 * 排序测试
	 */
	@Test
	public void test() throws InterruptedException {
		jedis.rpush("a", "1");
		jedis.lpush("a", "6");
		jedis.lpush("a", "3");
		jedis.lpush("a", "9");
		System.out.println(jedis.lrange("a", 0, -1));
		System.out.println(jedis.sort("a"));
	}
	/**
	 * publish:消息发布
	 */
	@Test
	public void publish() throws InterruptedException {
		//消息渠道
		String channel="news.share";
		//消息内容
		String message="share a link http://www.google.com";
		jedis.publish(channel, message);
	}
	/**
	 * publish:消息订阅
	 */
	@Test
	public void psubscribe() throws InterruptedException {
		
		JedisPubSub jedisPubSub=new JedisPubSub() {
			
			@Override
			public void onUnsubscribe(String channel, int subscribedChannels) {
				
			}
			
			@Override
			public void onSubscribe(String channel, int subscribedChannels) {
				
			}
			
			@Override
			public void onPUnsubscribe(String arg0, int arg1) {
				
			}
			
			@Override
			public void onPSubscribe(String arg0, int arg1) {
				System.out.println("onPUnsubscribe订阅频道=="+arg0);
			}
			
			@Override
			public void onPMessage(String arg0, String arg1, String arg2) {
				System.out.println("onPMessage订阅内容=="+arg0+arg1+arg2);
			}
			
			@Override
			public void onMessage(String channel, String message) {
				
			}
		};
		//订阅以news.开头的所有频道
		String patterns="news.*";
		jedis.psubscribe(jedisPubSub, patterns);
	}
	/**
	 * 关闭资源
	 */
	@After
	public void close() {
		RedisUtil.returnResource(jedis);
	}
}