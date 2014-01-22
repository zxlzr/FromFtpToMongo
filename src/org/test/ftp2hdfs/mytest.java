package org.test.ftp2hdfs;


import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;


public class mytest {

	public static void main(String[] args) throws Exception {  
    	  
    	  testUpload();
      }
     
        
        //�ϴ������ļ���HDFS
       
        public static void testUpload() throws Exception{
        	
               Configuration conf = new Configuration();
                //conf.addResource(new Path("E:\\hadoop\\hadoop-0.21.0\\conf\\hdfs-site.xml"));
               
         
                conf.set("fs.default.name","hdfs://10.214.0.144:9000");
                FileSystem hdfs = FileSystem.get(conf);
                Path src = new Path("D:\\tmp\\a.txt");
                Path dst = new Path("/user/hadoop/tmp");
                hdfs.copyFromLocalFile(src, dst);
                
                System.out.println("Upload to " + conf.get("fs.default.name"));
                FileStatus files[] = hdfs.listStatus(dst);
                for(FileStatus file : files){
                        System.out.println(file.getPath());
                }
                
        }
        
        //����HDFS�ļ�
     
        public void testCreate() throws Exception{
                
                Configuration conf = new Configuration();
                conf.addResource(new Path("D:\\myeclipse\\Hadoop\\hadoopEx\\src\\conf\\hadoop.xml"));
                
                byte[] buff = "hello world!".getBytes();
                
                FileSystem hdfs = FileSystem.get(conf);
                Path dst = new Path("/test");
                FSDataOutputStream outputStream = null;
                try{
                        outputStream = hdfs.create(dst);
                        outputStream.write(buff,0,buff.length);
                }catch(Exception e){
                        e.printStackTrace();
                        
                }finally{
                        if(outputStream != null){
                                outputStream.close();
                        }
                }
                
                FileStatus files[] = hdfs.listStatus(dst);
                for(FileStatus file : files){
                        System.out.println(file.getPath());
                }
        }
        
        //������HDFS�ļ�
   
        public void testRename() throws Exception{
                
                Configuration conf = new Configuration();
                conf.addResource(new Path("D:\\myeclipse\\Hadoop\\hadoopEx\\src\\conf\\hadoop.xml"));
                
                
                FileSystem hdfs = FileSystem.get(conf);
                Path dst = new Path("/");
                
                Path frpath = new Path("/test");
                Path topath = new Path("/test1");
                
                hdfs.rename(frpath, topath);
                
                FileStatus files[] = hdfs.listStatus(dst);
                for(FileStatus file : files){
                        System.out.println(file.getPath());
                }
        }
        
        //�h��HDFS�ļ�
   
        public void testDel() throws Exception{
                
                Configuration conf = new Configuration();
                conf.addResource(new Path("D:\\myeclipse\\Hadoop\\hadoopEx\\src\\conf\\hadoop.xml"));
                
                
                FileSystem hdfs = FileSystem.get(conf);
                Path dst = new Path("/");
                
                Path topath = new Path("/test1");
                
                boolean ok = hdfs.delete(topath,false);
                System.out.println( ok ? "ɾ���ɹ�" : "ɾ��ʧ��");
                
                FileStatus files[] = hdfs.listStatus(dst);
                for(FileStatus file : files){
                        System.out.println(file.getPath());
                }
        }
        
        //�鿴HDFS�ļ�������޸�ʱ��
     
        public void testgetModifyTime() throws Exception{
                
                Configuration conf = new Configuration();
                conf.addResource(new Path("D:\\myeclipse\\Hadoop\\hadoopEx\\src\\conf\\hadoop.xml"));
                
                
                FileSystem hdfs = FileSystem.get(conf);
                Path dst = new Path("/");
                
                FileStatus files[] = hdfs.listStatus(dst);
                for(FileStatus file : files){
                        System.out.println(file.getPath() +"\t" + file.getModificationTime());
                }
        }
        
        //�鿴HDFS�ļ��Ƿ����
     
        public void testExists() throws Exception{
                
                Configuration conf = new Configuration();
                conf.addResource(new Path("D:\\myeclipse\\Hadoop\\hadoopEx\\src\\conf\\hadoop.xml"));
                
                
                FileSystem hdfs = FileSystem.get(conf);
                Path dst = new Path("/T.txt");
                
                boolean ok  = hdfs.exists(dst);
                System.out.println( ok ? "�ļ�����" : "�ļ�������");
        }
        
        //�鿴ĳ���ļ���HDFS��Ⱥ��λ��
   
        public void testFileBlockLocation() throws Exception{
                
                Configuration conf = new Configuration();
                conf.addResource(new Path("D:\\myeclipse\\Hadoop\\hadoopEx\\src\\conf\\hadoop.xml"));
                
                
                FileSystem hdfs = FileSystem.get(conf);
                Path dst = new Path("/T.txt");
                
                FileStatus fileStatus =  hdfs.getFileStatus(dst);
                BlockLocation[] blockLocations =hdfs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
                for(BlockLocation block : blockLocations){
                        System.out.println(Arrays.toString(block.getHosts())+ "\t" + Arrays.toString(block.getNames()));
                }
        }
        
        //��ȡHDFS��Ⱥ�����нڵ�����
      
        public void testGetHostName() throws Exception{
                
                Configuration conf = new Configuration();
                conf.addResource(new Path("D:\\myeclipse\\Hadoop\\hadoopEx\\src\\conf\\hadoop.xml"));
                
                
                DistributedFileSystem hdfs = (DistributedFileSystem)FileSystem.get(conf);
                DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
                
                for(DatanodeInfo dataNode : dataNodeStats){
                        System.out.println(dataNode.getHostName() + "\t" + dataNode.getName());
                }
        }

}
