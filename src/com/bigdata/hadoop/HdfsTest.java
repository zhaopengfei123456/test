package com.bigdata.hadoop;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
/*
 * Ȩ������ �������� hdfs dfs  -chmod 777 /
 */
public class HdfsTest {
	
	static final String ROOT = "hdfs://192.168.207.129:9000"; 
    static final Configuration config = new Configuration(); 
    /**      
     * ��ȡFileSystemʵ��      
     * @return      
     * @throws Exception      
     */        
    public static FileSystem getFileSystem() throws Exception{         
    	return FileSystem.get(new URI(ROOT), config);     
    }
    /** 
     * �����ļ��� 
     * @param path    ���·������"/test/" 
     * @return �Ƿ񴴽��ɹ� 
     * @throws Exception 
     */
    public static boolean Mkdir(String path) throws Exception { 
        FileSystem fileSystem = getFileSystem(); 
        boolean isCreate = fileSystem.mkdirs(new Path(path)); 
        return isCreate; 
    }
    
    /**      
     * 
     * ɾ���ļ���     
     * @param path �ļ���·��     
     * @return   �Ƿ�ɹ�ɾ��     
     */    
    public static boolean Removedir(String path) {         
    	boolean result = false;         
    	Path strPath = new Path(path);         
    	try {             
    		FileSystem fileSystem = getFileSystem();             
    		if(fileSystem.isDirectory(strPath)){                 
    			result = fileSystem.delete(strPath);             
    	    }else{                 
    	    	throw new NullPointerException("�߼��쳣��ɾ���ļ������Գ���δ��ɾ���ļ��У����ɾ���ļ�Ӧʹ��deleteFile(String path)");             
    	    }         
    	} catch (Exception e) {             
    			System.out.println("�쳣��" + e.getMessage());         
    	}         
    	return result;     
    }
	
    /**      
     *ɾ���ļ�      
     *@param path    
     *�ļ�·��      
     *@return    
     *�Ƿ�ɹ�ɾ��     
     */    
    public static boolean deleteFile(String path) {         
    	boolean result = false;         
    	Path strPath = new Path(path);         
    	try {             
    		FileSystem fileSystem = getFileSystem();             
    		if(fileSystem.isFile(strPath)){                 
    			result = fileSystem.delete(strPath);             
    		}else{                 
    			throw new NullPointerException("�߼��쳣��ɾ���ļ����Գ���δ��ɾ���ļ������ɾ���ļ���Ӧʹ��Removedir(String path)");             
    		}         
    	} catch (Exception e) {             
    			System.out.println("�쳣��" + e.getMessage());         
    	}         
    	return result;     
    }
    /**      
     * ���ļ����ļ����б�      
     * @param path �ļ����ļ���·��   
     * @return����FileStatus[]      
     */    
    public static FileStatus[] getList(String path){         
    	return getList(path,false);             
    }
    
    /**   
     * ���ļ����ļ����б�      
     * @param path  
     * @param isFull    true:�ݹ�����ļ���·����ȡ��false������ȡ��һ�㲻���еݹ����   
     * @return      
     * */    
    public static FileStatus[] getList(String path,boolean isFull){         
    	List<FileStatus> arrayList = new ArrayList<FileStatus>();         
    	Path strPath = new Path(path);         
    	try {             
    		FileSystem fileSystem = getFileSystem();             
    		FileStatus[] result = fileSystem.listStatus(strPath);             
    		for (int i = 0; i < result.length; i++) {                 
    			FileStatus item = result[i];                 
    			arrayList.add(item);                                     
    			if(item.isDir()){                     
    				FileStatus[] results = getList(item.getPath().toString(),true);                     
    				for (int j = 0; j < results.length; j++) {                         
    					FileStatus fileStatus = results[j];                         
    					arrayList.add(fileStatus);                     
    					}                 
    				}                               
    			}         
    		} catch (Exception e) {             
    			System.out.println("�쳣��" + e.getMessage());         
    		}         
    	FileStatus[] results = new FileStatus[arrayList.size()];         
    	arrayList.toArray(results);         
    	return results;     
    }
    /**       
     * �ϴ��ļ�      
     * @param src ���ش����ļ�     
     * @param tar    �ϴ��ļ���HDFS·��     
     * @return �Ƿ�ɹ��ϴ�     
     *  */    
    public static boolean PutFile(String src,String tar) {         
    	boolean result = false;         
    	Path srcPath = new Path(src);         
    	Path tarPath = new Path(tar);         
    	try {             
    		FileSystem fileSystem = getFileSystem();             
    		fileSystem.copyFromLocalFile(srcPath, tarPath);             
    		result = true;         
    		} catch (Exception e) {             
    			System.out.println("�쳣��" + e.getMessage());         
    		}         
    	return result;     
    }
    /**      
     * �����ļ������ش���     
     * @param path    HDFS·��     
     * @return ���ر��ش���·��     
     */    
    public static String DownFile(String path){             
    	int index = path.lastIndexOf('.') > 0 ? path.lastIndexOf('.') : 0;         
    	String name = path.substring(index);         
    	SimpleDateFormat format=new SimpleDateFormat("yyyyMMddHHmmss");
    	System.out.println("user.dir=="+System.getProperty("user.dir"));
    	String target =String.format("{0}\\{1}{2}", System.getProperty("user.dir"),format.format(new Date()), name);         
    	Path strPath = new Path(path);         
    	Path tarPath = new Path(target);         
    	try {             
    		FileSystem fileSystem = getFileSystem();             
    		fileSystem.copyToLocalFile(strPath, tarPath);         
    	} catch (Exception e) {             
    			e.printStackTrace();        
    	}         
    	return target;     
    }
    
    public static void main(String[] args) throws Exception {                
    	String strDir = "/data/";        
    	String strDirback = "/data/back";        
    	String strFile = "E:\\hellohadoop.txt";        
    	String strPutFilePath = "/data/hellohadoop.txt";        
    	if(HdfsTest.Mkdir(strDir)){            
    		if(HdfsTest.PutFile(strFile, strPutFilePath)){                
    			if(HdfsTest.Mkdir(strDirback)){                    
    				FileStatus[] list = HdfsTest.getList(strDir);                    
    				for (int i = 0; i < list.length; i++) {                        
    					FileStatus fileStatus = list[i];                        
    					final String name = fileStatus.getPath().getName();                        
    					final String path = fileStatus.getPath().toString();                        
    					final long length = fileStatus.getLen();                        
    					final String dir = fileStatus.isDir() ? "d" : "-";                        
    					final short replication = fileStatus.getReplication();                        
    					final String permission = fileStatus.getPermission().toString();                        
    					final String group = fileStatus.getGroup();                        
    					final String owner = fileStatus.getOwner();                        
    					System.out.println(dir + permission + "\t" + replication  + "\t" + group + "\t" + owner + "\t" + path);
    				}                                     
    			}            
    		}        
    	}
    	//ɾ���ļ�
    	deleteFile(strPutFilePath);
    	Removedir(strDir);
    	
    	//DownFile(strPutFilePath);
    	
    } 
}
