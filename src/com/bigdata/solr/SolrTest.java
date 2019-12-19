package com.bigdata.solr;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

public class SolrTest {
	 // solr服务的url，mycore 是前面创建的solr core
	 public String url = "http://localhost:8983/solr/mycore";
	/**
	 * 添加/修改索引
	 * 在solr中，索引库中都会存在一个唯一键。
	 * 如果一个Document的id存在，则执行修改操作，如果不存在，则执行添加操作
	 */
	@Test
	public void insertOrUpdateIndex(){
	    // 创建HttpSolrClient
	    HttpSolrClient client = new HttpSolrClient.Builder(url)
	        .withConnectionTimeout(5000)
	        .withSocketTimeout(5000)
	        .build();
	    // 创建Document对象
	    SolrInputDocument document = new SolrInputDocument();
	    document.addField("id", "2222");
	    document.addField("title", "Solr测试");
	    document.addField("user_name", "必须的字段名称");
	    document.addField("sellPoint", "Solr版本差异也太大了 手机 电子产品");
	    try {
			client.add(document);
			client.commit();
			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	/**
	 * 删除索引
	 */
	@Test
	public void deleteIndex() {
		HttpSolrClient client = new HttpSolrClient.Builder(url)
				.withConnectionTimeout(5000)
				.withSocketTimeout(5000)
				.build();
		try {
			// 根据指定ID来删除
			//client.deleteById("1111");
			// 根据条件删除
			//client.deleteByQuery("id:1111");
			// 全部删除
			//client.deleteByQuery("*:*");
			
			client.commit();
			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * 简单查询
	 */
	@Test
    public void simpleSearch()  {
        HttpSolrClient client = new HttpSolrClient.Builder(url)
                .withConnectionTimeout(5000)
                .withSocketTimeout(5000)
                .build();
        // 创建SolrQuery
        SolrQuery query = new SolrQuery();
        // 输入查询条件
        query.setQuery("text:培训");
        //query.setQuery("id:1111");
        //query.setQuery("*:*");
        // 执行查询并返回结果
        QueryResponse response;
		try {
			response = client.query(query);
			// 获取匹配的所有结果
	        SolrDocumentList list = response.getResults();
	        // 匹配结果总数
	        long count = list.getNumFound();
	        System.out.println("总结果数：" + count);
	 
	        for (SolrDocument document : list) {
	            System.out.println(document.get("id"));
	            System.out.println(document.get("title"));
	            System.out.println(document.get("sellPoint"));
	            System.out.println(document.get("user_name"));
	            System.out.println(document.get("u_age"));
	            System.out.println(document.get("text"));
	            System.out.println(document.get("fileAbsolutePath"));
	            System.out.println("================");
	        }
	        client.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
	/**
	 * 复杂条件查询
	 */
	@Test
	public void complexSearch() throws IOException, SolrServerException {
	    HttpSolrClient client = new HttpSolrClient.Builder(url)
	        .withConnectionTimeout(5000)
	        .withSocketTimeout(5000)
	        .build();
	    SolrQuery query = new SolrQuery();
	    // 输入查询条件
	    query.setQuery("sellPoint:电子 AND sellPoint:手机");
	    // 设置过滤条件
	    //query.setFilterQueries("id:[1 TO 1200000]");
	    // 设置排序
	    query.addSort("id", SolrQuery.ORDER.desc);
	    // 设置分页信息（使用默认的）
	    query.setStart(0);
	    query.setRows(10);
	    // 设置显示的Field的域集合(两种方式二选一)
	    // query.setFields(new String[]{"id", "title", "sellPoint", "price", "status" });
	    query.setFields("id,title,user_name,sellPoint");
	    // 设置默认域
	    // query.set("df", "product_keywords");
	    // 设置高亮信息
	    query.setHighlight(true);
	    query.addHighlightField("sellPoint");
	    query.setHighlightSimplePre("<span color='red'>");
	    query.setHighlightSimplePost("</span>");
	 
	    // 执行查询并返回结果
	    QueryResponse response = client.query(query);
	    // 获取匹配的所有结果
	    SolrDocumentList list = response.getResults();
	    // 匹配结果总数
	    long count = list.getNumFound();
	    System.out.println("总结果数：" + count);
	 
	    // 获取高亮显示信息
	    Map<String, Map<String, List<String>>> highlighting = response.getHighlighting();
	    for (SolrDocument document : list) {
	        System.out.println(document.get("id"));
	        List<String> list2 = highlighting.get(document.get("id")).get("sellPoint");
	        if (list2 != null)
	            System.out.println("高亮显示的商品名称：" + list2.get(0));
	        else {
	            System.out.println(document.get("title"));
	        }
	    }
	    
	    /** 
	     *  分组代码
	     *  solrQuery.setParam(GroupParams.GROUP,true);
			solrQuery.setParam(GroupParams.GROUP_FIELD,"id");
			// 设置每个quality对应的
			solrQuery.setParam(GroupParams.GROUP_LIMIT,"1");
			 
			GroupResponse groupResponse =queryResponse.getGroupResponse();
			if(groupResponse !=null) {
			     List<GroupCommand> groupList =groupResponse.getValues();
			     for(GroupCommand groupCommand : groupList){
			          List<Group> groups =groupCommand.getValues();
			          for(Group group : groups) {
			            System.out.println(group.getGroupValue()+"\t"+group.getResult().getNumFound());
			          }
			     }
			}
			
			//查询分组数量
			solrQuery.setFacet(true);
			solrQuery.setFacetLimit(100);
			solrQuery.setFacetMissing(false);
			solrQuery.addFacetField("id");
			 
			List<FacetField.Count> counts;
			List<FacetField> facetFieldList = queryResponse.getFacetFields();
			for (FacetField facetField : facetFieldList) {
			    System.out.println(facetField.getName()+"\t"+facetField.getValueCount());
			    counts = facetField.getValues();
			    if (counts != null) {
			        for (FacetField.Count count : counts) {
			             System.out.println(count.getName()+" "+count.getCount());
			         }
			    }
			}
	     */
	    client.close();
	}

}
