package net.xfok.ftp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.jcraft.jsch.Channel;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SftpException;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;


public class MySFTP {

/**
* 连接sftp服务器
* @param host 主机
* @param port 端口
* @param username 用户名
* @param password 密码
* @return
*/
public ChannelSftp connect(String host, int port, String username,
String password) {
ChannelSftp sftp = null;
try {
JSch jsch = new JSch();
jsch.getSession(username, host, port);
Session sshSession = jsch.getSession(username, host, port);
System.out.println("Session created.");
sshSession.setPassword(password);
Properties sshConfig = new Properties();
sshConfig.put("StrictHostKeyChecking", "no");
sshSession.setConfig(sshConfig);
sshSession.connect();
System.out.println("Session connected.");
System.out.println("Opening Channel.");
Channel channel = sshSession.openChannel("sftp");
channel.connect();
sftp = (ChannelSftp) channel;
System.out.println("Connected to " + host + ".");
} catch (Exception e) {

}
return sftp;
}

/**
* 上传文件
* @param directory 上传的目录
* @param uploadFile 要上传的文件
* @param sftp
*/
public void upload(String directory, String uploadFile, ChannelSftp sftp) {
try {
sftp.cd(directory);
File file=new File(uploadFile);
sftp.put(new FileInputStream(file), file.getName());
} catch (Exception e) {
e.printStackTrace();
}
}

/**
* 下载文件
* @param directory 下载目录
* @param downloadFile 下载的文件
* @param saveFile 存在本地的路径
* @param sftp
*/
public void download(String directory, String downloadFile,String saveFile, ChannelSftp sftp) {
try {
sftp.cd(directory);
File file=new File(saveFile);
sftp.get(downloadFile, new FileOutputStream(file));
} catch (Exception e) {
e.printStackTrace();
}
}

/**
* 删除文件
* @param directory 要删除文件所在目录
* @param deleteFile 要删除的文件
* @param sftp
*/
public void delete(String directory, String deleteFile, ChannelSftp sftp) {
try {
sftp.cd(directory);
sftp.rm(deleteFile);
} catch (Exception e) {
e.printStackTrace();
}
}

/**
* 列出目录下的文件
* @param directory 要列出的目录
* @param sftp
* @return
* @throws SftpException
*/
public Vector listFiles(String directory, ChannelSftp sftp) throws SftpException{
return sftp.ls(directory);
}
public void getnewfile(String saveFile,String thismonth,String thisday,ChannelSftp sftp) throws SftpException{

	
	
	
	
	
	
}

public static void getdata(){
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
}
public static void getftpdata() throws SftpException, IOException{
	
	Configuration conf = new Configuration();
	conf.set("fs.default.name","hdfs://10.214.0.144:9000");

	FileSystem hdfs = FileSystem.get(conf);

	Path src;
	Path dst;
	MySFTP sf = new MySFTP(); 
	String host = "lz3.verizon.com";
	int port = 22;
	String username = "sftpBridgevine";
	String password = "!6CePdP5Vn#4$";

	//String saveFile = "/home/hadoop/";//linux
	String saveFile = "D:\\tmp\\";

	String filename = null;
	SimpleDateFormat df = new SimpleDateFormat("MMM d",Locale.ENGLISH);//
	String thismonth=df.format(new Date()).split(" ")[0];
	String thisday=df.format(new Date()).split(" ")[1];

 


	ChannelSftp sftp=sf.connect(host, port, username, password);
	

	
	
	
	
	//第一个文件夹
	sftp.cd("Order Status");
	System.out.println("change to Order Status");
	
	Vector vecFields = new Vector();
	vecFields=sf.listFiles(".", sftp);
	
	for(int i = 0;i < vecFields.size();i++){ 

		
	String tmp=vecFields.get(i).toString();
	//System.out.println(tmp);
	
	String Timetmp=tmp.substring(42,48);//length 

	String thatmonth=Timetmp.split("\\s")[0];
	String thatday=Timetmp.trim().substring(4,Timetmp.trim().length());
	//System.out.println(thatmonth);

	//System.out.println(thatday);

	if(thatmonth.equals(thismonth)){
		if(thatday.equals(thisday)){
			
			String[] strs=tmp.split("ORDERSTATUS");
			filename="ORDERSTATUS"+strs[1];
			System.out.println("downloading    "+filename);
		   sf.download(".",filename, saveFile+filename, sftp);
		   System.out.println("store in "+saveFile+filename);
		   src = new Path(saveFile+filename);
		    dst = new Path("/user/hadoop/upload");
		    hdfs.copyFromLocalFile(src, dst);
		    
		    System.out.println("Upload to " + conf.get("fs.default.name"));
			
		}
		
	}
	
	
	}


	
	sftp.cd("../DTV Order Status");
	System.out.println("change to DTV Order Status");
	vecFields=sf.listFiles(".", sftp);

	for( int i = 0;i < vecFields.size();i++){ 


	 String tmp = vecFields.get(i).toString();
	//System.out.println(tmp);
	String  Timetmp=tmp.substring(42,48);//length 

	String thatmonth=Timetmp.split("\\s")[0];
    String thatday=Timetmp.trim().substring(4,Timetmp.trim().length());
	//System.out.println(thatmonth);

	//System.out.println(thatday);

	if(thatmonth.equals(thismonth)){
		if(thatday.equals(thisday)){
			
			String[] strs=tmp.split("ORDERSTATUS");
			filename="ORDERSTATUS"+strs[1];
			System.out.println("downloading    "+filename);
		   sf.download(".",filename, saveFile+filename, sftp);
		   System.out.println("store in "+saveFile+filename);
		   src = new Path(saveFile+filename);
		    dst = new Path("/user/hadoop/upload");
		    hdfs.copyFromLocalFile(src, dst);
		    
		    System.out.println("Upload to " + conf.get("fs.default.name"));
			
		}
		
	}
	
	
	
	}
	
	
	
    Mongo mongo = new Mongo("10.214.0.144", 27017);//链接配置
    //连接名为wiretap的数据库，假如数据库不存在的话，mongodb会自动建立
    DB db = mongo.getDB("test");
    System.out.println("Get MongoDB ");
// Get collection from MongoDB, database named "yourDB"
//从Mongodb中获得名为yourColleection的数据集合，如果该数据集合不存在，Mongodb会为其新建立
    DBCollection collection = db.getCollection("FtpData");


    String[] keys=new String[100];
    String[] values=new String[100];
//int b=0;
    String line="";
    String keyline="";

     String  remotefile="hdfs://10.214.0.144:9000/user/hadoop/download/orderstatus.txt";//hdfs文件
     FileSystem fs = FileSystem.get(URI.create(remotefile), conf);
     Path path = new Path(remotefile);
     FSDataInputStream in = fs.open(path);
     BufferedReader br = new BufferedReader(new InputStreamReader(in));


	

	  try {
	  
     
	  
	   keyline =br.readLine();
	   keys=keyline.split(",");
	  // System.out.print(keys[0]+"\n");
	   while((line=br.readLine())!=null){
		   values=line.split(",");
		   BasicDBObject document = new BasicDBObject();
		   for(int i=0;i<values.length;i++)
		   {
			  
		        document.put(keys[i],values[i]);
		      
		        //将新建立的document保存到collection中去
		     
		   }
		   collection.insert(document);
			 
		
	    
	   }
	   System.out.println("put the "+remotefile+" data into MondoDB");
	   fs.close();

	  } catch (Exception e) {
	   e.printStackTrace();
	  }
	
	
	
	
   

		

	
	
	
	
	
}

public static void main(String[] args) throws IOException, SftpException, ParseException {


	String Starttime="";
if(args.length==0)
{//没输出时间，默认系统时间
	Starttime="15:00:00";
	System.out.println("The system will get ftp data at 15:00:00 everyday");
}
	
else 
	{
	
	
	Starttime=args[0];
	System.out.println("The system will get ftp data at "+Starttime+" everyday");
	}


getftpdata();
//一天的毫秒数
long daySpan = 24 * 60 * 60 * 1000;

// 规定的每天时间15:33:30运行
final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd '"+Starttime+"'");
// 首次运行时间
Date startTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(sdf.format(new Date()));

// 如果今天的已经过了 首次运行时间就改为明天
if(System.currentTimeMillis() > startTime.getTime())
startTime = new Date(startTime.getTime() + daySpan);

Timer t = new Timer();

TimerTask task = new TimerTask(){
@Override
public void run() {
	
System.err.println("begin");
	try {
		getftpdata();
	} catch (SftpException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	

	
	
	
	//System.err.println("xxxxxxxxx");
	

}
};

// 以每24小时执行一次
t.scheduleAtFixedRate(task, startTime, daySpan);






   
	
	
	
	
	
 
	
	
	
	
	
	
	
/*
	
	
   

    
    */
    

} 
}




