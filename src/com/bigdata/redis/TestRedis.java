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
	 * �滻�������� \d{1,}[ ]{4}
	 */
	private Jedis jedis;

	@Before
	public void setup() {
		// ����redis��������192.168.0.100:6379
		// jedis = new Jedis("127.0.0.1", 6379);
		// Ȩ����֤
		//jedis.auth("admin");
		jedis = RedisUtil.getJedis();
	}
	/**
	 * �������ݿ�
	 */
	@Test
	public void testdb() {
		//�������ݿ⣬Ĭ�Ͼ���0
		jedis.select(0);
		//ͨ��ͨ�����ȡ����key
		Set<String> keys=jedis.keys("*");
		Iterator<String> it=keys.iterator();
		System.out.println("��ǰ���ݿ�"+jedis.getDB()+",��ȡ����key,����="+jedis.dbSize());
		int k=0;
		while(it.hasNext()){
			String key=it.next();
			k++;
			System.out.println(k+" key ����="+key+"������="+jedis.type(key));
		}
		//�ж�ָ����key�Ƿ����
		System.out.println("�Ƿ��������Ϊname��key="+jedis.exists("name"));
		//ɾ��ָ����key
		long n=jedis.del("name","key1");
		System.out.println("ɾ����"+n+"��key");
		//���ص�ǰ���ݿ��е����key
		System.out.println("��ǰ���ݿ��е����key="+jedis.randomKey());
		//������һ��key,���newkey���ڣ�ֵ���ᱻ���ǣ�����1��ʾ�ɹ���0ʧ��
		jedis.mset("key1","000","key2","001");
		System.out.println("key1��ֵ��="+jedis.get("key1")+",key2��ֵ��="+jedis.get("key2"));
		jedis.rename("key1", "key2");//��key1������Ϊkey2,key1�������ڣ�key2��ֵ��key1ԭ����ֵ
		System.out.println("��������key1��ֵ��="+jedis.get("key1")+",key2��ֵ��="+jedis.get("key2"));
		//Ϊkeyָ������ʱ�䣬��λ����
		jedis.expire("key2", 30);
		System.out.println("key2����ڻ�ʣ  "+jedis.ttl("key2")+"��");
		//ɾ����ǰ���ݿ�������key
		System.out.println("ɾ����ǰ���ݿ�������key="+jedis.flushDB());
	    //ɾ���������ݿ��е�����key
		System.out.println("ɾ���������ݿ��е�����key="+jedis.flushAll());
	}
    /**
    * redis�洢�ַ���
    */
	@Test
	public void testString() {
		// -----�������----------
		jedis.set("name", "�����");
		// ���key�Ѿ����ڣ�����0
		long k = jedis.setnx("name", "����");
		System.out.println(jedis.get("name") + "k==" + k);
		// getset key value ԭ�ӵ�����key��ֵ��������key�ľ�ֵ�����key�����ڷ���nil
		String v1 = jedis.getSet("name", "����");
		System.out.println("jedis.get(name)=" + jedis.get("name") + ", getset������==" + v1);
		// ƴ��
		jedis.append("name", " is my lover");
		System.out.println("ƴ�Ӻ��ֵ��=" + jedis.get("name"));
		// ɾ��ĳ����
		jedis.del("name");
		System.out.println("ɾ����name���ֵ��=" + jedis.get("name"));
		// ���ö����ֵ��
		jedis.mset("name", "�����", "age", "35", "qq", "476777XXX");
		// ���м�1����
		jedis.incr("age");
		// ���м�1����
		jedis.decr("age");
		// ��age��ָ��ֵ
		jedis.incrBy("age", 10);
		// ��age��ָ��ֵ
		jedis.decrBy("age", 15);
		System.out.println("name����Ӧ��value����=" + jedis.strlen("name"));
		// ��ȡkey���ַ���ֵ,�����޸�key��ֵ���±��Ǵ�0��ʼ��
		System.out.println("��ȡ���ֵ��" + jedis.substr("name", 0, 2));
		// ��ȡ�������ֵ
		List<String> lists = jedis.mget("name", "age", "qq");
		System.out.println("----�����ֵ���---------");
		for (String value : lists) {
			System.out.println(value);
		}
	}
   
	/**
	 * redis����Map
	 */
	@Test
	public void testMap() {
		// ��ֵ���
		jedis.hset("userinfo", "name", "�����");
		// ��ֵȡֵ
		System.out.println("ȡ����ֵ=" + jedis.hget("userinfo", "name"));
		// ��Ӷ��ֵ
		Map<String, String> map = new HashMap<String, String>();
		map.put("name", "xinxin");
		map.put("age", "22");
		map.put("qq", "123456");
		jedis.hmset("user", map);
		// ��ָ����hash filed ���ϸ���ֵ
		jedis.hincrBy("user", "age", 14);
		// ��ȡ���ֵ
		List<String> rsmap = jedis.hmget("user", "name", "age", "qq");
		System.out.println("��ȡ���ֵ=" + rsmap);
		// ɾ��map�е�ĳ����ֵ
		jedis.hdel("user", "age");
		// ����ָ��field�Ƿ����
		System.out.println("����ָ��ageԪ���Ƿ����" + jedis.hexists("user", "age"));
		// ����keyΪuser�ļ��д�ŵ�ֵ�ĸ���
		System.out.println("user�ļ��д�ŵ�ֵԪ�ظ���=" + jedis.hlen("user"));
		// �Ƿ����keyΪuser�ļ�¼
		System.out.println("�Ƿ����keyΪuser�ļ�¼=" + jedis.exists("user"));
		// ����map�����е�����key
		System.out.println("����map�����е�����key=" + jedis.hkeys("user"));
		// ����map�����е�����value
		System.out.println("����map�����е�����value=" + jedis.hvals("user"));
		// ����ָ��key������filed��value
		Map<String, String> maps = jedis.hgetAll("user");
		Set<String> keys = maps.keySet();
		for (String key : keys) {
			System.out.println("key=" + key + ",value=" + maps.get(key));
		}
	}
   
	/**
	 * jedis����List
	 */
	@Test
	public void testList() {
		// ����� javaframework��,��ͷ������ַ���Ԫ�� �����������
		jedis.lpush("javaframework", "spring");
		jedis.lpush("javaframework", "struts");
		jedis.lpush("javaframework", "hibernate");
		// ����� javaframework��,��β������ַ���Ԫ�� �����������
		jedis.rpush("javaframework", "mybitis");
		jedis.rpush("javaframework", "springMVC");
		jedis.rpush("javaframework", "springboot");
		// ��ȡԪ�ظ���
		System.out.println("list��javaframework��Ԫ�ظ�����=" + jedis.llen("javaframework"));
		// ��ȡlist������ָ��������Ԫ�أ��ɹ�����1��key�����ڷ��ش���,��ǰҲ����
		jedis.ltrim("javaframework", 0, 6);
		// lset key index value ����list��ָ���±��Ԫ��ֵ���ɹ�����1��key�����±겻���ڷ��ش���
		jedis.lset("javaframework", 0, "springclood");
		// ��key��Ӧlist��ɾ��count����value��ͬ��Ԫ�ء�countΪ0ʱ��ɾ��ȫ��
		jedis.lrem("javaframework", 1, "springclood");
		// lpop key ��list��ͷ��ɾ��Ԫ�أ�������ɾ��Ԫ��
		jedis.lpop("javaframework");
		// rpop key ��list��β��ɾ��Ԫ�أ�������ɾ��Ԫ��
		jedis.rpop("javaframework");
				
		/**
		 * blpop timeout key1...keyN ������ɨ�践�ضԵ�һ���ǿ�list����lpop���������أ� ����blpop 0
		 * list1 list2 list3
		 * ,���list1������list2,list3���Ƿǿ�,���list2��lpop�����ش�list2��ɾ����Ԫ�ء�
		 * ������е�list���ǿջ򲻴��ڣ��������timeout�룬timeoutΪ0��ʾһֱ����,
		 * ������ʱ�������client��key1...keyN�е�����key����push����,���һ�����key�ϱ�������client���������ء�
		 * �����ʱ�������򷵻�nil 
		 * brpop ͬblpop��һ���Ǵ�ͷ��ɾ��һ���Ǵ�β��ɾ��
		 */
		jedis.blpop(2, "javaframework");
		// ����list�е�����value
		List<String> lists = jedis.lrange("javaframework", 0, jedis.llen("javaframework"));
		int k = 0;
		System.out.println("----����list----");
		for (String value : lists) {
			k++;
			System.out.println(k + " list�и���Ԫ�ص�ֵ��=" + value);
		}
		// ɾ��javaframework ��
		jedis.del("javaframework");
	}  
	/**
	 * jedis����Set
	 */
	@Test
	public void testSet() {
		// ���
		jedis.sadd("sset1", "����");
		jedis.sadd("sset1", "����");
		jedis.sadd("sset1", "�ŷ�");
		jedis.sadd("sset1", "����");
		jedis.sadd("sset1", "����");
		jedis.sadd("sset1", "000");
		jedis.sadd("sset2", "000");
		jedis.sadd("sset2", "001");
		jedis.sadd("sset3", "a");
		jedis.sadd("sset3", "b");
		// �Ƴ�����
		jedis.srem("sset1", "����");
		// ɾ��������key��Ӧset�������һ��Ԫ��
		System.out.println("ɾ��������key��Ӧset�������һ��Ԫ��=" + jedis.spop("sset1"));
		// ���ȡset�е�һ��Ԫ�أ����ǲ�ɾ��Ԫ��
		System.out.println("���ȡset�е�һ��Ԫ��=" + jedis.srandmember("sset1"));
		// ��sset1���Ƴ� Ԫ�ز���ӵ�sset2��Ӧset��
		jedis.smove("sset1", "sset2", "����");
		// �ж�Ԫ���Ƿ���set�У����ڷ���1,���򷵻�0
		System.out.println("sset2���Ƿ��������=" + jedis.sismember("sset2", "����"));
		// ��ȡԪ�ظ���
		System.out.println("sset1Ԫ�ظ�����=" + jedis.scard("sset1") + ",sset2Ԫ�ظ�����=" + jedis.scard("sset2"));
		// �������и���key�Ľ���
		System.out.println("-------�������------");
		Set<String> sinners = jedis.sinter("sset1", "sset2");
		Iterator<String> innerit = sinners.iterator();
		while (innerit.hasNext()) {
			System.out.println("������Ԫ����=" + innerit.next());
		}
		// �������и���key�Ľ���,ͬʱ�������浽sset3��
		System.out.println("-------�������Ľ���------");
		jedis.sinterstore("sset3", "sset1", "sset2");
		System.out.println("sset3������Ԫ����=" + jedis.smembers("sset3"));
		// �������и���key�Ĳ���
		System.out.println("-------�������------");
		Set<String> sunions = jedis.sunion("sset1", "sset2");
		for (String value : sunions) {
			System.out.println("������Ԫ����=" + value);
		}
		// ���ز������ұ��浽ָ����sset3����
		System.out.println("-------�������Ĳ���------");
		jedis.sunionstore("sset3", "sset1", "sset2");
		System.out.println("sset3������Ԫ����=" + jedis.smembers("sset3"));
		// �������и���key�Ĳ
		System.out.println("-------����------");
		Set<String> diffs = jedis.sdiff("sset1", "sset2");
		for (String difvalue : diffs) {
			System.out.println("���Ԫ����=" + difvalue);
		}
		// ���ز���ұ��浽ָ����sset3����
		System.out.println("-------�������Ĳ------");
		jedis.sdiffstore("sset3", "sset1", "sset2");
		System.out.println("sset3������Ԫ����=" + jedis.smembers("sset3"));
	}  
	/**
	 * jedis����SortedSet
	 */
	@Test
	public void testSortedSet() {
		Map<Double, String> map=new HashMap<Double, String>();
		map.put(1.0,"java");
		map.put(2.0,"c#");
		map.put(3.0,"c++");
		map.put(-1.0,"python");
		//���
		jedis.zadd("zset",map);
		//ɾ��
		jedis.zrem("zset", "python");
		//����Ԫ�ص�scoreֵ�����ظ��º��scoreֵ
		jedis.zincrby("zset", 4, "java");
		//����ָ��Ԫ���ڼ����е�����,������Ԫ���ǰ�score��С���������
		System.out.println("java�ڼ����д�С���������="+jedis.zrank("zset", "java"));
		//����ָ��Ԫ���ڼ����е�����,������Ԫ���ǰ�score�Ӵ�С�����
		System.out.println("java�ڼ����дӴ��С������="+jedis.zrevrank("zset", "java"));
		//zrange���������zrevrange �������
		System.out.println("����score����������="+jedis.zrange("zset", 0, 10));
		System.out.println("����score����������="+jedis.zrevrange("zset", 0, 10));
		//���ؼ�����score�ڸ��������Ԫ��
		System.out.println("���ؼ�����score�ڸ��������Ԫ�ر������="+jedis.zrangeByScore("zset", "3", "4"));
		//���ؼ�����score�ڸ������������
		System.out.println("������score�ڸ���������������="+jedis.zcount("zset", "3", "4"));
		//���ؼ�����Ԫ�ظ���
		System.out.println("������Ԫ�ظ�����="+jedis.zcard("zset"));
		//���ظ���Ԫ�ض�Ӧ��score
		System.out.println("����Ԫ��java��Ӧ��scoreֵ��="+jedis.zscore("zset", "java"));
		//ɾ�������������ڸ��������Ԫ��
		System.out.println("ɾ�������������ڸ��������Ԫ�ؽ����="+jedis.zremrangeByRank("zset", 0,1));
		//ɾ��������score�ڸ��������Ԫ��
		System.out.println("ɾ��������score�ڸ��������Ԫ�ؽ����="+jedis.zremrangeByScore("zset", 0,1));
	}
	/**
	 * �������
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
	 * publish:��Ϣ����
	 */
	@Test
	public void publish() throws InterruptedException {
		//��Ϣ����
		String channel="news.share";
		//��Ϣ����
		String message="share a link http://www.google.com";
		jedis.publish(channel, message);
	}
	/**
	 * publish:��Ϣ����
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
				System.out.println("onPUnsubscribe����Ƶ��=="+arg0);
			}
			
			@Override
			public void onPMessage(String arg0, String arg1, String arg2) {
				System.out.println("onPMessage��������=="+arg0+arg1+arg2);
			}
			
			@Override
			public void onMessage(String channel, String message) {
				
			}
		};
		//������news.��ͷ������Ƶ��
		String patterns="news.*";
		jedis.psubscribe(jedisPubSub, patterns);
	}
	/**
	 * �ر���Դ
	 */
	@After
	public void close() {
		RedisUtil.returnResource(jedis);
	}
}