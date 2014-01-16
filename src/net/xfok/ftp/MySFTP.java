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
* ����sftp������
* @param host ����
* @param port �˿�
* @param username �û���
* @param password ����
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
* �ϴ��ļ�
* @param directory �ϴ���Ŀ¼
* @param uploadFile Ҫ�ϴ����ļ�
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
* �����ļ�
* @param directory ����Ŀ¼
* @param downloadFile ���ص��ļ�
* @param saveFile ���ڱ��ص�·��
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
* ɾ���ļ�
* @param directory Ҫɾ���ļ�����Ŀ¼
* @param deleteFile Ҫɾ�����ļ�
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
* �г�Ŀ¼�µ��ļ�
* @param directory Ҫ�г���Ŀ¼
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
	

	
	
	
	
	//��һ���ļ���
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
	
	
	
    Mongo mongo = new Mongo("10.214.0.144", 27017);//��������
    //������Ϊwiretap�����ݿ⣬�������ݿⲻ���ڵĻ���mongodb���Զ�����
    DB db = mongo.getDB("test");
    System.out.println("Get MongoDB ");
// Get collection from MongoDB, database named "yourDB"
//��Mongodb�л����ΪyourColleection�����ݼ��ϣ���������ݼ��ϲ����ڣ�Mongodb��Ϊ���½���
    DBCollection collection = db.getCollection("FtpData");


    String[] keys=new String[100];
    String[] values=new String[100];
//int b=0;
    String line="";
    String keyline="";

     String  remotefile="hdfs://10.214.0.144:9000/user/hadoop/download/orderstatus.txt";//hdfs�ļ�
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
		      
		        //���½�����document���浽collection��ȥ
		     
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
{//û���ʱ�䣬Ĭ��ϵͳʱ��
	Starttime="15:00:00";
	System.out.println("The system will get ftp data at 15:00:00 everyday");
}
	
else 
	{
	
	
	Starttime=args[0];
	System.out.println("The system will get ftp data at "+Starttime+" everyday");
	}


getftpdata();
//һ��ĺ�����
long daySpan = 24 * 60 * 60 * 1000;

// �涨��ÿ��ʱ��15:33:30����
final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd '"+Starttime+"'");
// �״�����ʱ��
Date startTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(sdf.format(new Date()));

// ���������Ѿ����� �״�����ʱ��͸�Ϊ����
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

// ��ÿ24Сʱִ��һ��
t.scheduleAtFixedRate(task, startTime, daySpan);






   
	
	
	
	
	
 
	
	
	
	
	
	
	
/*
	
	
   

    
    */
    

} 
}




