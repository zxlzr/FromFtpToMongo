package net.xfok.ftp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
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
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
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
public static void getftpdata(int starttime) throws SftpException, IOException{
	
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

	//String saveFile = "/home/hadoop/tmp/";//linux
	String saveFile = "D:\\tmp\\";//windows
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

	
			String[] strs=tmp.split("ORDERSTATUS");
			filename="ORDERSTATUS"+strs[1];
			int j;
			
			if((j=filename.indexOf("ORDERSTATUS_MOVERS_BN"))!=-1){
				//System.out.println("find file "+filename);
				String Timestamp=filename.substring(22,30);
				int timenow=Integer.parseInt(Timestamp);
				if(timenow>=starttime){
					System.out.println("start to download file from "+starttime);
					//System.out.println(Timestamp);
					System.out.println("downloading    "+filename);
				   sf.download(".",filename, saveFile+filename, sftp);
				   System.out.println("store in "+saveFile+filename);
				   
				   File f1 = new File(saveFile+filename);
				   File f2 = new File(saveFile+"orderstatus_verizon_"+Timestamp+".txt");
				   String[] values = new String[100];
				   FileReader reader = new FileReader(f1);
				   FileWriter writer = new FileWriter(f2);
				   BufferedReader br = new BufferedReader(reader);
				   BufferedWriter bw = new BufferedWriter(writer);
			       String line="";
			       String keyline =br.readLine();
			       String[] keys=keyline.split("~");
			     
			     
			      // String writekeys="DateTime\001CHANNEL\001VENDOR";
			       //bw.write(writekeys);
			      // bw.newLine();
		   	   //bw.flush();
			       //begin
			       while((line=br.readLine())!=null){
			    	 line=line+"\001";
			    		
			    	    values=line.split("\\~");//文件时间以后在弄
			    	 //  System.out.println(line);
			    	 // /  for(int i=0;i<values.length;i++){
			    	    //	System.out.println(values[i]);
			    	    //	}//main
			    	    StringBuffer tmp1 = new StringBuffer("");
			    	    String aaa=Timestamp.substring(0,4);
			    	   // System.out.println(aaa);
			    	    String bbb=Timestamp.substring(4,6);
			    	   // System.out.println(bbb);
			    	    String ccc=Timestamp.substring(6,8);
			    	    //System.out.println(ccc);
			    	    
			    	    tmp1.append(aaa+"-"+bbb+"-"+ccc+" 12:00:00"+"\001");//DateTime
			    	    tmp1.append(values[0]+"\001");//CHANNEL
			    	    tmp1.append(values[1]+"\001");//VENDOR
			    	    tmp1.append("BV"+"\001");//PARTNER
			    	    tmp1.append(values[2]+"\001");//REP_LNAME
			    	    tmp1.append(values[3]+"\001");//REP_FNAME
			    	    tmp1.append(values[4]+"\001");//STORE_ID
			    	    tmp1.append(values[5]+"\001");//SOURCE
			    	    tmp1.append(values[6]+"\001");//BTN
			    	    tmp1.append(values[7]+"\001");//CBR
			    	    tmp1.append(values[8]+"\001");//CUST_LNAME
			    	    tmp1.append(values[9]+"\001");//CUST_FNAME
			    	    tmp1.append(values[10]+"\001");//ADDRESS
			    	    tmp1.append(values[11]+"\001");//CITY
			    	    tmp1.append(values[12]+"\001");//STATE
			    	    tmp1.append(values[13]+"\001");//ZIP
			    	    tmp1.append(values[14]+"\001");//EMAIL
			    	    tmp1.append(values[15]+"\001");//SALES_DATE
			    	    tmp1.append(values[16]+"\001");//ORDER_NBR
			    	    tmp1.append(values[17]+"\001");//MON
			    	    tmp1.append(values[18]+"\001");//USOC
			    	    tmp1.append(values[19]+"\001");//PRODUCT_GROUP
			    	    tmp1.append(values[20]+"\001");//PRODUCT_ID
			    	    tmp1.append(values[21]+"\001");//PRODUCT
			    	    tmp1.append(values[22]+"\001");//QTY
			    	    tmp1.append(values[23]+"\001");//INSTALL_DATE
			    	    tmp1.append(values[24]+"\001");//CANCEL_DATE
			    	    
			    	    tmp1.append(values[25]+"\001");//CANCEL_TYPE
			    	    tmp1.append(values[26]+"\001");//CANCEL_REASON1
			    	    tmp1.append(values[27]+"\001");//CANCEL_REMARKS
			    	  
			    	   //是否最新 order number
			    	
			    	
			    	    if(!values[23].equals("")){
			    	    	if(!values[24].equals("")){
			    	    	   tmp1.append("disconnected"+"\001");
			    	    	}
			    	    	else{
			    	    	   tmp1.append("installed"+"\001");
			    	    	}
			    	    }
			    	    else{
			    	        if(!values[24].equals("")){
			    	    	   tmp1.append("cancelled"+"\001");
			    	    	}
			    	    	else{
			    	    	   tmp1.append("submitted"+"\001");
			    	    	}
			    	    }
			    	    tmp1.append(values[28]+"\001");//SEL_DUE_DATE
			    	    tmp1.append(values[29]+"\001");//ONT
			    	    tmp1.append(values[30]+"\001");//SELF_INSTALL
			    	  
			    	     tmp1.append(values[31]+"\001");//eONT_Flag
			    	    tmp1.append(values[32].replace("\001","")+"\001");//SI_Offered
			    	   //  System.out.println(values[32]);
			    	    
			    	    bw.write(tmp1.toString());
			    	    bw.newLine();
			    	    bw.flush();
			    	    
			    	   }
			    	   reader.close();
			    	   writer.close();
					
					
					
					
				}
				else {
					System.out.println("there is no new files");
					
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
	         String[] strs=tmp.split("ORDERSTATUS");
	         filename="ORDERSTATUS"+strs[1];
			int j;
			if((j=filename.indexOf("ORDERSTATUS_MOVERS_DTV"))!=-1){
				String Timestamp=filename.substring(23,31);
				int timenow=Integer.parseInt(Timestamp);
				System.out.println(timenow);
				if(timenow>=starttime){
					Timestamp=filename.substring(23,31);
				
					System.out.println("start to download file from "+starttime);
					System.out.println("downloading    "+filename);
				   sf.download(".",filename, saveFile+filename, sftp);
				   System.out.println("store in "+saveFile+filename);
				   
				   File f1 = new File(saveFile+filename);
				   File f2 = new File(saveFile+"orderstatus_verizon_"+Timestamp+".txt");
				   System.out.println(saveFile+"orderstatus_verizon_"+Timestamp+".txt");
				   String[] values = new String[100];
				   FileReader reader = new FileReader(f1);
				   FileWriter writer = new FileWriter(f2,true);
				   BufferedReader br = new BufferedReader(reader);
				   BufferedWriter bw = new BufferedWriter(writer);
			       String line="";
			       String keyline =br.readLine();
			       String[] keys=keyline.split("~");
			     
			     
			      // String writekeys="DateTime\001CHANNEL\001VENDOR";
			       //bw.write(writekeys);
			      // bw.newLine();
		   	   //bw.flush();
			       //begin
			       while((line=br.readLine())!=null){
			    	 line=line+"\001";
			    		
			    	    values=line.split("\\~");//文件时间以后在弄
			    	   // System.out.println(values[3]);
			    	 // /  for(int i=0;i<values.length;i++){
			    	    //	System.out.println(values[i]);
			    	    //	}//main
			    	    StringBuffer tmp1 = new StringBuffer("");
			    	    String aaa=Timestamp.substring(0,4);
				    	   // System.out.println(aaa);
				    	    String bbb=Timestamp.substring(4,6);
				    	   // System.out.println(bbb);
				    	    String ccc=Timestamp.substring(6,8);
				    	    //System.out.println(ccc);
				    	    
				    	    tmp1.append(aaa+"-"+bbb+"-"+ccc+" 12:00:00"+"\001");//DateTime
			    	    tmp1.append(values[0]+"\001");//CHANNEL
			    	    tmp1.append(values[1]+"\001");//VENDOR
			    	    tmp1.append("DTV"+"\001");//PARTNER
			    	    tmp1.append(values[2]+"\001");//REP_LNAME
			    	    tmp1.append(values[3]+"\001");//REP_FNAME
			    	    tmp1.append(values[4]+"\001");//STORE_ID
			    	    tmp1.append(values[5]+"\001");//SOURCE
			    	    tmp1.append(values[6]+"\001");//BTN
			    	    tmp1.append(values[7]+"\001");//CBR
			    	    tmp1.append(values[8]+"\001");//CUST_LNAME
			    	    tmp1.append(values[9]+"\001");//CUST_FNAME
			    	    tmp1.append(values[10]+"\001");//ADDRESS
			    	    tmp1.append(values[11]+"\001");//CITY
			    	    tmp1.append(values[12]+"\001");//STATE
			    	    tmp1.append(values[13]+"\001");//ZIP
			    	    tmp1.append(values[14]+"\001");//EMAIL
			    	    tmp1.append(values[15]+"\001");//SALES_DATE
			    	    tmp1.append(values[16]+"\001");//ORDER_NBR
			    	    tmp1.append(values[17]+"\001");//MON
			    	    tmp1.append(values[18]+"\001");//USOC
			    	    tmp1.append(values[19]+"\001");//PRODUCT_GROUP
			    	    tmp1.append(values[20]+"\001");//PRODUCT_ID
			    	    tmp1.append(values[21]+"\001");//PRODUCT
			    	    tmp1.append(""+"\001");//QTY        
			    	    tmp1.append(values[22]+"\001");//INSTALL_DATE
			    	    tmp1.append(values[23]+"\001");//CANCEL_DATE
			    	    tmp1.append(values[24]+"\001");//CANCEL_TYPE
			    	    
			    	    tmp1.append(values[25]+"\001");//CANCEL_REASON1
			    	    tmp1.append(values[26].replace("\001","")+"\001");//CANCEL_REMARKS
			    	  
			    	  
			    	 
			    	
			    	
			    	    if(!values[22].equals("")){
			    	    	if(!values[23].equals("")){
			    	    	
			    	    	   tmp1.append("disconnected"+"\001");
			    	    	}
			    	    	else{
			    	    	   tmp1.append("installed"+"\001");
			    	    	}
			    	    }
			    	    else{
			    	        if(!values[23].equals("")){
			    	    	   tmp1.append("cancelled"+"\001");
			    	    	}
			    	    	else{
			    	    	   tmp1.append("submitted"+"\001");
			    	    	}
			    	    } //ORDERSTATUS
			    	    tmp1.append(""+"\001");//SEL_DUE_DATE
			    	    tmp1.append(""+"\001");//ONT
			    	    tmp1.append(""+"\001");//SELF_INSTALL
			    	  
			    	     tmp1.append(""+"\001");//eONT_Flag
			    	    tmp1.append(""+"\001");//SI_Offered
			    	   //  System.out.println(values[32]);
			    	    
			    	    bw.write(tmp1.toString());
			    	    bw.newLine();
			    	    bw.flush();
			    	    
			    	   }
			    	   reader.close();
			    	   writer.close();
			    	      src = new Path(saveFile+"orderstatus_verizon_"+Timestamp+".txt");
					      dst = new Path("/user/hadoop/Verizon");
					   hdfs.copyFromLocalFile(src, dst);
					  System.out.println("Upload "+saveFile+"orderstatus_verizon_"+Timestamp+".txt"+"to  " + conf.get("fs.default.name"));
					  //File file = new File(saveFile+"orderstatus_verizon_"+Timestamp+".txt");
					 // file.delete();
					
				   
					
					
				}
				else {
					System.out.println("there is no new files");
					
				}
				
			}
		
		   
		   
			
		
		   
		   
		   
		   
		   
		   
		   
		   
		   
		   
		   
		   
		   
		   
		   
		   
		   
		   
		   
		 //  src = new Path(saveFile+filename);
		   // dst = new Path("/user/hadoop/upload");
		  //  hdfs.copyFromLocalFile(src, dst);
		    
		   // System.out.println("Upload to " + conf.get("fs.default.name"));

	
	
	
	}
	
	//上传文件到hdfs
	//删除临时文件
	
	
	

		

	System.out.println("all files have been put to hdfs,have fun! ");
	
	
	
	
}

public static void hdfs2mongo(int from,int to) throws IOException{
	
	Configuration conf = new Configuration();
	conf.set("fs.default.name","hdfs://10.214.0.144:9000");
	
    Mongo mongo = new Mongo("10.214.0.144", 27017);//链接配置
    //连接名为wiretap的数据库，假如数据库不存在的话，mongodb会自动建立
    DB db = mongo.getDB("maize");
    System.out.println("Get MongoDB ");
// Get collection from MongoDB, database named "yourDB"
//从Mongodb中获得名为yourColleection的数据集合，如果该数据集合不存在，Mongodb会为其新建立
    DBCollection collection = db.getCollection("VerizonOrderStatus");


    String[] keys=new String[100];
    String[] values=new String[100];
//int b=0;
    String line="";
    String keyline="";
   
    for(int k=from;k<to;k++)
	   {
	  
    	
    	 
    	 String  remotefile="hdfs://10.214.0.144:9000/user/hadoop/Verizon/OrderStatusUpdate/day="+Integer.toString(k)+"/000000_0";//hdfs文件
         FileSystem fs = FileSystem.get(URI.create(remotefile), conf);
         Path path = new Path(remotefile);
         FSDataInputStream in = fs.open(path);
         BufferedReader br = new BufferedReader(new InputStreamReader(in));

         System.out.print("get file"+Integer.toString(k));
    	

    	  try {
    	  
         
    	  
    	 //  keyline =br.readLine();
    	//   keys=keyline.split(",");
    	  
    	   while((line=br.readLine())!=null){
    		   line=line+"~";
    		   values=line.split("\001");
    		   
    		   BasicDBObject document = new BasicDBObject();
    		  // System.out.println(line);
    		   for(int i=0;i<values.length;i++)
    		   {
    			   // System.out.println(values[i]);
    			    document.put("DateTime",values[0]);
    			    document.put("CHANNEL",values[1]);
    			    document.put("VENDOR",values[2]);
    			    document.put("PARTNER",values[3]);
    			    document.put("REP_LNAME",values[4]);
    			    document.put("REP_FNAME",values[5]);
    			    document.put("STORE_ID",values[6]);
    			    document.put("SOURCE",values[7]);
    			    document.put("BTN",values[8]);
    			    document.put("CBR",values[9]);
    			    document.put("CUST_LNAME",values[10]);
    			    document.put("CUST_FNAME",values[11]);
    			    document.put("ADDRESS",values[12]);
    			    document.put("CITY",values[13]);
    			    document.put("STATE",values[14]);
    			    document.put("ZIP",values[15]);
    			    document.put("EMAIL",values[16]);
    			    document.put("SALES_DATE",values[17]);
    			    document.put("ORDER_NBR",values[18]);
    			    document.put("MON",values[19]);
    			    document.put("USOC",values[20]);
    			    document.put("PRODUCT_GROUP",values[21]);
    			    document.put("PRODUCT_ID",values[22]);
    			    document.put("PRODUCT",values[23]);
    			    document.put("QTY",values[24]);
    			    document.put("INSTALL_DATE",values[25]);
    			    document.put("CANCEL_DATE",values[26]);
    			    document.put("CANCEL_TYPE",values[27]);
    			    document.put("CANCEL_REASON1",values[28]);
    			    document.put("CANCEL_REMARKS",values[29]);
    			    document.put("ORDERSTATUS",values[30]);
    			    document.put("SEL_DUE_DATE",values[31]);
    			    document.put("ONT",values[32]);
    			    document.put("SELF_INSTALL",values[33]);
    			    document.put("eONT_Flag",values[34]);
    			    document.put("SI_Offered",values[35]);
    			   
    			   
    			   
    			   
    			   
    			   
    			   
    			   
    			   
    			   
    			  
    		       // document.put(keys[i],values[i]);
    		      
    		        //将新建立的document保存到collection中去
    		     
    		   }
    		   //做查询
    		  
    		   BasicDBObject obj = new BasicDBObject();
    	        obj.put("ORDER_NBR",values[18]);	
    	        obj.put("MON",values[19]);
    	        obj.put("PRODUCT_ID",values[22]);
    	        obj.put("islatest",1);
    	       
    	        DBObject cursor = collection.findOne(obj);
    	        if(cursor!=null){
    	       String  ORDERSTATUS= (String) cursor.get("ORDERSTATUS");
    	        //System.out.println("find " + ORDERSTATUS);
    	      // System.out.println(ORDERSTATUS+""+values[30]);
    	      if(ORDERSTATUS.equals(values[30]))
    	        //cursor.close();
    	      { 
    		   
    		   document.put("islatest",0);
    		   System.out.println("same");
    	      }
    	      else{
    	    	  System.out.println("updated");
    	    	  System.out.println(values[18]);
    	    	  collection.update(new BasicDBObject("islatest",0),cursor);
    	    	  document.put("islatest",1); 
    	    	  collection.insert(document);
    	      }
    	        }else{
    	        	System.out.println("new");
    	        	 document.put("islatest",1); 
    	        	 collection.insert(document);
    	        }
    	      
    	    //  document.put("islatest",1); 
    		   
    		 //  collection.insert(document);
    		
    	    
    	   }
    	   System.out.println("put the "+remotefile+" data into MondoDB");
    	   fs.close();

    	  } catch (Exception e) {
    	   e.printStackTrace();
    	  }
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
    	
	   }

    
	
   

	
	
	
	
	
}
public static int filestarttime;
public static void main(String[] args) throws IOException, SftpException, ParseException {


	String Starttime="";
	//后期由系统产生
	
	
if(args.length==0)
{//没输出时间，默认系统时间
	Starttime="23:40:00";
	filestarttime=20140115;
	System.out.println("usage:Ftp2Hdfs_fat.jar 15:00:00 20140113\n");
	System.out.println("The system will get ftp data at 15:00:00 everyday and get data from time"+filestarttime);
}
	
else 
	{
    filestarttime=Integer.parseInt(args[1]);
	
	Starttime=args[0];
	//filestarttime=Integer.parseInt(args[1]);
	System.out.println("The system will get ftp data at "+Starttime+" everyday and get data from time"+filestarttime);
	
	}
//getftpdata(20131220);
hdfs2mongo(20131221,20131222);

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
		getftpdata(filestarttime);// need to change
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




