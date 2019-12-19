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
	 // solr�����url��mycore ��ǰ�洴����solr core
	 public String url = "http://localhost:8983/solr/mycore";
	/**
	 * ���/�޸�����
	 * ��solr�У��������ж������һ��Ψһ����
	 * ���һ��Document��id���ڣ���ִ���޸Ĳ�������������ڣ���ִ����Ӳ���
	 */
	@Test
	public void insertOrUpdateIndex(){
	    // ����HttpSolrClient
	    HttpSolrClient client = new HttpSolrClient.Builder(url)
	        .withConnectionTimeout(5000)
	        .withSocketTimeout(5000)
	        .build();
	    // ����Document����
	    SolrInputDocument document = new SolrInputDocument();
	    document.addField("id", "2222");
	    document.addField("title", "Solr����");
	    document.addField("user_name", "������ֶ�����");
	    document.addField("sellPoint", "Solr�汾����Ҳ̫���� �ֻ� ���Ӳ�Ʒ");
	    try {
			client.add(document);
			client.commit();
			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	/**
	 * ɾ������
	 */
	@Test
	public void deleteIndex() {
		HttpSolrClient client = new HttpSolrClient.Builder(url)
				.withConnectionTimeout(5000)
				.withSocketTimeout(5000)
				.build();
		try {
			// ����ָ��ID��ɾ��
			//client.deleteById("1111");
			// ��������ɾ��
			//client.deleteByQuery("id:1111");
			// ȫ��ɾ��
			//client.deleteByQuery("*:*");
			
			client.commit();
			client.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * �򵥲�ѯ
	 */
	@Test
    public void simpleSearch()  {
        HttpSolrClient client = new HttpSolrClient.Builder(url)
                .withConnectionTimeout(5000)
                .withSocketTimeout(5000)
                .build();
        // ����SolrQuery
        SolrQuery query = new SolrQuery();
        // �����ѯ����
        query.setQuery("text:��ѵ");
        //query.setQuery("id:1111");
        //query.setQuery("*:*");
        // ִ�в�ѯ�����ؽ��
        QueryResponse response;
		try {
			response = client.query(query);
			// ��ȡƥ������н��
	        SolrDocumentList list = response.getResults();
	        // ƥ��������
	        long count = list.getNumFound();
	        System.out.println("�ܽ������" + count);
	 
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
	 * ����������ѯ
	 */
	@Test
	public void complexSearch() throws IOException, SolrServerException {
	    HttpSolrClient client = new HttpSolrClient.Builder(url)
	        .withConnectionTimeout(5000)
	        .withSocketTimeout(5000)
	        .build();
	    SolrQuery query = new SolrQuery();
	    // �����ѯ����
	    query.setQuery("sellPoint:���� AND sellPoint:�ֻ�");
	    // ���ù�������
	    //query.setFilterQueries("id:[1 TO 1200000]");
	    // ��������
	    query.addSort("id", SolrQuery.ORDER.desc);
	    // ���÷�ҳ��Ϣ��ʹ��Ĭ�ϵģ�
	    query.setStart(0);
	    query.setRows(10);
	    // ������ʾ��Field���򼯺�(���ַ�ʽ��ѡһ)
	    // query.setFields(new String[]{"id", "title", "sellPoint", "price", "status" });
	    query.setFields("id,title,user_name,sellPoint");
	    // ����Ĭ����
	    // query.set("df", "product_keywords");
	    // ���ø�����Ϣ
	    query.setHighlight(true);
	    query.addHighlightField("sellPoint");
	    query.setHighlightSimplePre("<span color='red'>");
	    query.setHighlightSimplePost("</span>");
	 
	    // ִ�в�ѯ�����ؽ��
	    QueryResponse response = client.query(query);
	    // ��ȡƥ������н��
	    SolrDocumentList list = response.getResults();
	    // ƥ��������
	    long count = list.getNumFound();
	    System.out.println("�ܽ������" + count);
	 
	    // ��ȡ������ʾ��Ϣ
	    Map<String, Map<String, List<String>>> highlighting = response.getHighlighting();
	    for (SolrDocument document : list) {
	        System.out.println(document.get("id"));
	        List<String> list2 = highlighting.get(document.get("id")).get("sellPoint");
	        if (list2 != null)
	            System.out.println("������ʾ����Ʒ���ƣ�" + list2.get(0));
	        else {
	            System.out.println(document.get("title"));
	        }
	    }
	    
	    /** 
	     *  �������
	     *  solrQuery.setParam(GroupParams.GROUP,true);
			solrQuery.setParam(GroupParams.GROUP_FIELD,"id");
			// ����ÿ��quality��Ӧ��
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
			
			//��ѯ��������
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
