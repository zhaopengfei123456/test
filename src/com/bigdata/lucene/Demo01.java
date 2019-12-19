package com.bigdata.lucene;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.nio.file.FileSystems;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.Scorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.pdfbox.io.RandomAccessBuffer;
import org.apache.pdfbox.io.RandomAccessRead;
import org.apache.pdfbox.pdfparser.PDFParser;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.xwpf.extractor.XWPFWordExtractor;
import org.apache.poi.xwpf.usermodel.XWPFDocument;

class Index {
    // ��������
    public void index() {
        IndexWriter indexWriter = null;
        try {
            // 1������Directory
            Directory directory = FSDirectory.open(FileSystems.getDefault().getPath("e:/LuceneDemo/data/index"));
            // 2������IndexWriter
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
            indexWriter = new IndexWriter(directory, indexWriterConfig);
            //�����ǰ��index
            indexWriter.deleteAll();
            //Ҫ������File·��
            File dFile = new File("e:/LuceneDemo/data/data");
            File[] files = dFile.listFiles();
            for (File file : files) {
            	 // 3������Document����
                Document document = new Document();
                InputStream in = new FileInputStream(file);
            	String fileName = file.getName();
            	// ��ȡ�ļ���׺����������Ϊ�ļ�����
                String fileType = fileName.substring(fileName.lastIndexOf(".") + 1,fileName.length()).toLowerCase();
                if("doc".equals(fileType)){
                	// ��ȡdoc��word�ĵ�
                    WordExtractor wordExtractor = new WordExtractor(in);
                    document.add(new Field("content",  wordExtractor.getText(), TextField.TYPE_NOT_STORED));
                }else if("docx".equals(fileType)){
                	XWPFDocument xwpfdocument=new XWPFDocument(in);
                	XWPFWordExtractor xwpfWordExtractor = new XWPFWordExtractor(xwpfdocument);
                	document.add(new Field("content",xwpfWordExtractor.getText(), TextField.TYPE_NOT_STORED));
                }else if("pdf".equals(fileType)){
                	RandomAccessRead ra=new RandomAccessBuffer(in);
                	PDFParser parser = new PDFParser(ra);
                    parser.parse();
                    PDDocument pdDocument = parser.getPDDocument();
                    PDFTextStripper stripper = new PDFTextStripper();
                	document.add(new Field("content",stripper.getText(pdDocument), TextField.TYPE_NOT_STORED));
                	pdDocument.close();
                }else if("txt".equals(fileType)){
                	document.add(new Field("content", new FileReader(file), TextField.TYPE_NOT_STORED));
                }
                // 4��ΪDocument���Field
                document.add(new Field("filename", file.getName(), TextField.TYPE_STORED));
                document.add(new Field("filepath", file.getAbsolutePath(), TextField.TYPE_STORED));
                // 5��ͨ��IndexWriter����ĵ���������
                indexWriter.addDocument(document);
                in.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (indexWriter != null) {
                    indexWriter.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

class Search {  
  
    public void search(String keyWord) {  
        DirectoryReader directoryReader = null;  
        try {  
            // 1������Directory  
        	Directory directory = FSDirectory.open(FileSystems.getDefault().getPath("e:/LuceneDemo/data/index"));
            // 2������IndexReader  
            directoryReader = DirectoryReader.open(directory);  
            // 3������IndexReader����IndexSearch  
            IndexSearcher indexSearcher = new IndexSearcher(directoryReader);  

            // 4������������Query  
            Analyzer analyzer = new StandardAnalyzer();  
            // ����parser��ȷ��Ҫ�����ļ������ݣ���һ������Ϊ��������  
            QueryParser queryParser = new QueryParser("content", analyzer);  
            // ����Query��ʾ������Ϊcontent,�Դ��������������
            Query query = queryParser.parse(keyWord);
            //��ʽ���������ַ���
            Formatter formatter  = new SimpleHTMLFormatter("<font color='red'>", "</font>");
            Scorer fragmentScorer = new QueryScorer(query);
            Highlighter highlighter  = new Highlighter(formatter, fragmentScorer);
            
 	        //���������ֶΣ����� 
	        SortField sortField = new SortField("filename",Type.DOC,true);
	        //����һ��Sort����
	        Sort sort = new Sort();
	        //�����������
	        sort.setSort(sortField);
            
            // 5������searcher���� ���򷵻�ǰ10����¼  
            TopDocs topDocs = indexSearcher.search(query, 10,sort);
            System.out.println("���ҵ����ĵ��ܹ��У�"+topDocs.totalHits);

            // 6�����ݲ�ѯ����ƥ����ļ�¼
            ScoreDoc[] scoreDocs = topDocs.scoreDocs;
            for (ScoreDoc scoreDoc : scoreDocs) {  
            	int docid=scoreDoc.doc;
                // 7������ID��ȡ�ĵ�  
                Document document = indexSearcher.doc(docid); 
                String filename=document.get("filename");
                String filepath=document.get("filepath");
                // 8������Document�����ȡ��Ҫ��ֵ  
                System.out.println(filename + " " + filepath);
                
	           	String highTitle = highlighter.getBestFragment(analyzer, "filename",filename);
	           	String highContent= highlighter.getBestFragment(analyzer, "filepath",filepath);
	           	
	           	//��ӡ�������ַ���
	        	System.out.println("�������ַ��� title:"+highTitle);
	        	System.out.println("�������ַ���  content:"+highContent);
            }  

        } catch (Exception e) {  
            e.printStackTrace();  
        } finally {  
            try {  
                if (directoryReader != null) {  
                    directoryReader.close();  
                }  
            } catch (Exception e) {
                e.printStackTrace();  
            }  
        }  
    }  
}

public class Demo01 {
	
	public static void main(String[] args) {
		Index newIndex = new Index();
	    newIndex.index();
	    Search newSearch = new Search();
	    //newSearch.search("maven OR java");
	    newSearch.search("python");
	}

}
