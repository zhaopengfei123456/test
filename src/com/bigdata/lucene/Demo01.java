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
    // 建立索引
    public void index() {
        IndexWriter indexWriter = null;
        try {
            // 1、创建Directory
            Directory directory = FSDirectory.open(FileSystems.getDefault().getPath("e:/LuceneDemo/data/index"));
            // 2、创建IndexWriter
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
            indexWriter = new IndexWriter(directory, indexWriterConfig);
            //清除以前的index
            indexWriter.deleteAll();
            //要搜索的File路径
            File dFile = new File("e:/LuceneDemo/data/data");
            File[] files = dFile.listFiles();
            for (File file : files) {
            	 // 3、创建Document对象
                Document document = new Document();
                InputStream in = new FileInputStream(file);
            	String fileName = file.getName();
            	// 获取文件后缀名，将其作为文件类型
                String fileType = fileName.substring(fileName.lastIndexOf(".") + 1,fileName.length()).toLowerCase();
                if("doc".equals(fileType)){
                	// 获取doc的word文档
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
                // 4、为Document添加Field
                document.add(new Field("filename", file.getName(), TextField.TYPE_STORED));
                document.add(new Field("filepath", file.getAbsolutePath(), TextField.TYPE_STORED));
                // 5、通过IndexWriter添加文档到索引中
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
            // 1、创建Directory  
        	Directory directory = FSDirectory.open(FileSystems.getDefault().getPath("e:/LuceneDemo/data/index"));
            // 2、创建IndexReader  
            directoryReader = DirectoryReader.open(directory);  
            // 3、根据IndexReader创建IndexSearch  
            IndexSearcher indexSearcher = new IndexSearcher(directoryReader);  

            // 4、创建搜索的Query  
            Analyzer analyzer = new StandardAnalyzer();  
            // 创建parser来确定要搜索文件的内容，第一个参数为搜索的域  
            QueryParser queryParser = new QueryParser("content", analyzer);  
            // 创建Query表示搜索域为content,对传入参数进行搜索
            Query query = queryParser.parse(keyWord);
            //格式化高亮的字符串
            Formatter formatter  = new SimpleHTMLFormatter("<font color='red'>", "</font>");
            Scorer fragmentScorer = new QueryScorer(query);
            Highlighter highlighter  = new Highlighter(formatter, fragmentScorer);
            
 	        //创建排序字段，降序 
	        SortField sortField = new SortField("filename",Type.DOC,true);
	        //创建一个Sort排序
	        Sort sort = new Sort();
	        //添加排序条件
	        sort.setSort(sortField);
            
            // 5、根据searcher搜索 降序返回前10条记录  
            TopDocs topDocs = indexSearcher.search(query, 10,sort);
            System.out.println("查找到的文档总共有："+topDocs.totalHits);

            // 6、根据查询条件匹配出的记录
            ScoreDoc[] scoreDocs = topDocs.scoreDocs;
            for (ScoreDoc scoreDoc : scoreDocs) {  
            	int docid=scoreDoc.doc;
                // 7、根据ID获取文档  
                Document document = indexSearcher.doc(docid); 
                String filename=document.get("filename");
                String filepath=document.get("filepath");
                // 8、根据Document对象获取需要的值  
                System.out.println(filename + " " + filepath);
                
	           	String highTitle = highlighter.getBestFragment(analyzer, "filename",filename);
	           	String highContent= highlighter.getBestFragment(analyzer, "filepath",filepath);
	           	
	           	//打印高亮后字符串
	        	System.out.println("高亮后字符串 title:"+highTitle);
	        	System.out.println("高亮后字符串  content:"+highContent);
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
