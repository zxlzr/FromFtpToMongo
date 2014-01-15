package net.xfok.ftp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;

import com.jcraft.jsch.SftpException;

public class Makeonefile {
	public static void main(String[] args) throws IOException, SftpException, ParseException {

		String Timestamp="";
		  File f1 = new File("D:\\tmp\\ORDERSTATUS_MOVERS_BN_20140113.txt");
		   File f2 = new File("D:\\tmp\\test.txt");
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
	    	    tmp1.append(Timestamp+"12:00:00"+"\001");//DateTime
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
	    	  
	    	 
	    	
	    	
	    	    if(values[23]!=""){
	 			   
	    	    	  tmp1.append("Installed"+"\001");
	 			   
	 		   }
	    	    else if(values[24]!=""){
	    	    	tmp1.append("Cancelled"+"\001");
	    	    
	    	    	
	    	    }
	    	    else if(values[24]!=""&&values[23]!=""){
	    	    	tmp1.append("Disconnected"+"\001");
	    	    
	    	    	
	    	    	
	    	    }
	    	    else{
	    	    	tmp1.append(""+"\001");

	    	    	
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
		
		
		
		
		
	
	
}
