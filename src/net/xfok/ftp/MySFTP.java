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

import com.mongodb.DBObject;
import com.mongodb.Mongo;

public class MySFTP {

	/**
	 * 连接sftp服务器
	 * 
	 * @param host
	 *            主机
	 * @param port
	 *            端口
	 * @param username
	 *            用户名
	 * @param password
	 *            密码
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
	 * 
	 * @param directory
	 *            上传的目录
	 * @param uploadFile
	 *            要上传的文件
	 * @param sftp
	 */
	public void upload(String directory, String uploadFile, ChannelSftp sftp) {
		try {
			sftp.cd(directory);
			File file = new File(uploadFile);
			sftp.put(new FileInputStream(file), file.getName());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 下载文件
	 * 
	 * @param directory
	 *            下载目录
	 * @param downloadFile
	 *            下载的文件
	 * @param saveFile
	 *            存在本地的路径
	 * @param sftp
	 */
	public void download(String directory, String downloadFile,
			String saveFile, ChannelSftp sftp) {
		try {
			sftp.cd(directory);
			File file = new File(saveFile);
			sftp.get(downloadFile, new FileOutputStream(file));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 删除文件
	 * 
	 * @param directory
	 *            要删除文件所在目录
	 * @param deleteFile
	 *            要删除的文件
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
	 * 
	 * @param directory
	 *            要列出的目录
	 * @param sftp
	 * @return
	 * @throws SftpException
	 */
	public Vector listFiles(String directory, ChannelSftp sftp)
			throws SftpException {
		return sftp.ls(directory);
	}

	public void getnewfile(String saveFile, String thismonth, String thisday,
			ChannelSftp sftp) throws SftpException {

	}

	public static void getdata() {

	}

	public static void getftpdata(int starttime, int endtime)
			throws SftpException, IOException {
		FileInputStream fis = new FileInputStream(baseconf);

		Properties properties = new Properties();
		properties.load(fis);

		Configuration conf = new Configuration();
		String hdfs_server = properties.getProperty("hdfs_server",
				"hdfs://10.214.0.144:9000");

		conf.set("fs.defaultFS", hdfs_server);

		String remotefile = hdfs_server
				+ "/user/hadoop/Verizon/OrderStatusUpdate/day=20131222/000000_0";// hdfs文件
		FileSystem hdfs = FileSystem.get(URI.create(remotefile), conf);

		Path src;
		Path dst;
		MySFTP sf = new MySFTP();
		String host = properties
				.getProperty("ftp_server_ip", "lz3.verizon.com");
		int port = 22;
		String username = properties.getProperty("ftp_username",
				"sftpBridgevine");
		String password = properties.getProperty("ftp_password",
				"!6CePdP5Vn#4$");

		String saveFile = properties.getProperty("tmp_folder",
				"/home/hadoop/tmp/");// linux
		// String saveFile = "D:\\tmp\\";// windows
		String filename = null;
		SimpleDateFormat df = new SimpleDateFormat("MMM d", Locale.ENGLISH);//
		String thismonth = df.format(new Date()).split(" ")[0];
		String thisday = df.format(new Date()).split(" ")[1];

		ChannelSftp sftp = sf.connect(host, port, username, password);

		// 第一个文件夹
		sftp.cd("Order Status");
		System.out.println("change to Order Status");

		Vector vecFields = new Vector();
		vecFields = sf.listFiles(".", sftp);

		for (int i = 0; i < vecFields.size(); i++) {

			String tmp = vecFields.get(i).toString();
			// System.out.println(tmp);

			String Timetmp = tmp.substring(42, 48);// length

			String thatmonth = Timetmp.split("\\s")[0];
			String thatday = Timetmp.trim().substring(4,
					Timetmp.trim().length());
			// System.out.println(thatmonth);

			// System.out.println(thatday);

			String[] strs = tmp.split("ORDERSTATUS");
			filename = "ORDERSTATUS" + strs[1];
			int j;

			if ((j = filename.indexOf("ORDERSTATUS_INETACQ_BN")) != -1) {
				// System.out.println("find file "+filename);
				String Timestamp = filename.substring(filename.length() - 12,
						filename.length() - 4);
				int timenow = Integer.parseInt(Timestamp);
				// System.out.println(timenow);
				if (timenow >= starttime && timenow <= endtime) {
					System.out.println("start to download file from "
							+ starttime);
					// System.out.println(Timestamp);
					System.out.println("downloading    " + filename);
					sf.download(".", filename, saveFile + filename, sftp);
					System.out.println("store in " + saveFile + filename);

					File f1 = new File(saveFile + filename);
					File f2 = new File(saveFile + "orderstatus_verizon_"
							+ Timestamp + ".txt");
					String[] values = new String[100];
					FileReader reader = new FileReader(f1);
					FileWriter writer = new FileWriter(f2);
					BufferedReader br = new BufferedReader(reader);
					BufferedWriter bw = new BufferedWriter(writer);
					String line = "";
					String keyline = br.readLine();
					String[] keys = keyline.split("~");

					// String writekeys="DateTime\001CHANNEL\001VENDOR";
					// bw.write(writekeys);
					// bw.newLine();
					// bw.flush();
					// begin
					while ((line = br.readLine()) != null) {
						line = line + "\001";

						values = line.split("\\~");// 文件时间以后在弄
						// System.out.println(line);
						// / for(int i=0;i<values.length;i++){
						// System.out.println(values[i]);
						// }//main
						StringBuffer tmp1 = new StringBuffer("");
						String aaa = Timestamp.substring(0, 4);
						// System.out.println(aaa);
						String bbb = Timestamp.substring(4, 6);
						// System.out.println(bbb);
						String ccc = Timestamp.substring(6, 8);
						// System.out.println(ccc);

						tmp1.append(aaa + "-" + bbb + "-" + ccc + " 12:00:00"
								+ "\001");// DateTime
						tmp1.append(values[0] + "\001");// CHANNEL
						tmp1.append(values[1] + "\001");// VENDOR
						tmp1.append("verizon" + "\001");// PARTNER
						tmp1.append(values[2] + "\001");// REP_LNAME
						tmp1.append(values[3] + "\001");// REP_FNAME
						tmp1.append(values[4] + "\001");// STORE_ID
						tmp1.append(values[5] + "\001");// SOURCE
						tmp1.append(values[6] + "\001");// BTN
						tmp1.append(values[7] + "\001");// CBR
						tmp1.append(values[8] + "\001");// CUST_LNAME
						tmp1.append(values[9] + "\001");// CUST_FNAME
						tmp1.append(values[10] + "\001");// ADDRESS
						tmp1.append(values[11] + "\001");// CITY
						tmp1.append(values[12] + "\001");// STATE
						tmp1.append(values[13] + "\001");// ZIP
						tmp1.append(values[14] + "\001");// EMAIL
						tmp1.append(values[15] + "\001");// SALES_DATE
						tmp1.append(values[16] + "\001");// ORDER_NBR
						tmp1.append(values[17] + "\001");// MON
						tmp1.append(values[18] + "\001");// USOC
						tmp1.append(values[19] + "\001");// PRODUCT_GROUP
						tmp1.append(values[20] + "\001");// PRODUCT_ID
						tmp1.append(values[21] + "\001");// PRODUCT
						tmp1.append(values[22] + "\001");// QTY
						tmp1.append(values[23] + "\001");// INSTALL_DATE
						tmp1.append(values[24] + "\001");// CANCEL_DATE

						tmp1.append(values[25] + "\001");// CANCEL_TYPE
						tmp1.append(values[26] + "\001");// CANCEL_REASON1
						tmp1.append(values[27] + "\001");// CANCEL_REMARKS

						// 是否最新 order number

						if (!values[23].equals("")) {
							if (!values[24].equals("")) {
								tmp1.append("disconnected" + "\001");
							} else {
								tmp1.append("installed" + "\001");
							}
						} else {
							if (!values[24].equals("")) {
								tmp1.append("cancelled" + "\001");
							} else {
								tmp1.append("submitted" + "\001");
							}
						}
						tmp1.append(values[28] + "\001");// SEL_DUE_DATE
						tmp1.append(values[29] + "\001");// ONT
						tmp1.append(values[30] + "\001");// SELF_INSTALL

						tmp1.append(values[31] + "\001");// eONT_Flag
						tmp1.append(values[32].replace("\001", "") + "\001");// SI_Offered
						// System.out.println(values[32]);

						bw.write(tmp1.toString());
						bw.newLine();
						bw.flush();

					}
					reader.close();
					writer.close();

				}

			}

		}

		sftp.cd("../DTV Order Status");
		System.out.println("change to DTV Order Status");
		vecFields = sf.listFiles(".", sftp);
		int gflag=0;
		for (int i = 0; i < vecFields.size(); i++) {
			

			String tmp = vecFields.get(i).toString();
			// System.out.println(tmp);
			String Timetmp = tmp.substring(42, 48);// length

			String thatmonth = Timetmp.split("\\s")[0];
			String thatday = Timetmp.trim().substring(4,
					Timetmp.trim().length());
			// System.out.println(thatmonth);

			// System.out.println(thatday);
			String[] strs = tmp.split("ORDERSTATUS");
			filename = "ORDERSTATUS" + strs[1];
			int j;
			if ((j = filename.indexOf("ORDERSTATUS_INETACQ_DTV")) != -1) {
				// System.out.println(filename);
				String Timestamp = filename.substring(filename.length() - 12,
						filename.length() - 4);
				int timenow = Integer.parseInt(Timestamp);
				// System.out.println(timenow);
				if (starttime == endtime) {
				
					if (timenow == starttime) {
						gflag=1;
						filename="ORDERSTATUS_INETACQ_DTV_"+starttime+".txt";
						System.out.println("start to download file from "
								+ starttime);
						System.out.println("downloading    " + filename);
						sf.download(".", filename, saveFile + filename, sftp);
						System.out.println("store in " + saveFile + filename);

						File f1 = new File(saveFile + filename);
						File f2 = new File(saveFile + "orderstatus_verizon_"
								+ Timestamp + ".txt");
						System.out.println(saveFile + "orderstatus_verizon_"
								+ Timestamp + ".txt");
						String[] values = new String[100];
						FileReader reader = new FileReader(f1);
						FileWriter writer = new FileWriter(f2, true);
						BufferedReader br = new BufferedReader(reader);
						BufferedWriter bw = new BufferedWriter(writer);
						String line = "";
						String keyline = br.readLine();
						String[] keys = keyline.split("~");

						while ((line = br.readLine()) != null) {
							line = line + "\001";

							values = line.split("\\~");// 文件时间以后在弄
							// System.out.println(values[3]);
							// / for(int i=0;i<values.length;i++){
							// System.out.println(values[i]);
							// }//main
							StringBuffer tmp1 = new StringBuffer("");
							String aaa = Timestamp.substring(0, 4);
							// System.out.println(aaa);
							String bbb = Timestamp.substring(4, 6);
							// System.out.println(bbb);
							String ccc = Timestamp.substring(6, 8);
							// System.out.println(ccc);

							tmp1.append(aaa + "-" + bbb + "-" + ccc + " 12:00:00"
									+ "\001");// DateTime
							tmp1.append(values[0] + "\001");// CHANNEL
							tmp1.append(values[1] + "\001");// VENDOR
							tmp1.append("verizon" + "\001");// PARTNER
							tmp1.append(values[2] + "\001");// REP_LNAME
							tmp1.append(values[3] + "\001");// REP_FNAME
							tmp1.append(values[4] + "\001");// STORE_ID
							tmp1.append(values[5] + "\001");// SOURCE
							tmp1.append(values[6] + "\001");// BTN
							tmp1.append(values[7] + "\001");// CBR
							tmp1.append(values[8] + "\001");// CUST_LNAME
							tmp1.append(values[9] + "\001");// CUST_FNAME
							tmp1.append(values[10] + "\001");// ADDRESS
							tmp1.append(values[11] + "\001");// CITY
							tmp1.append(values[12] + "\001");// STATE
							tmp1.append(values[13] + "\001");// ZIP
							tmp1.append(values[14] + "\001");// EMAIL
							tmp1.append(values[15] + "\001");// SALES_DATE
							tmp1.append(values[16] + "\001");// ORDER_NBR
							tmp1.append(values[17] + "\001");// MON
							tmp1.append(values[18] + "\001");// USOC
							tmp1.append(values[19] + "\001");// PRODUCT_GROUP
							tmp1.append(values[20] + "\001");// PRODUCT_ID
							tmp1.append(values[21] + "\001");// PRODUCT
							tmp1.append("" + "\001");// QTY
							tmp1.append(values[22] + "\001");// INSTALL_DATE
							tmp1.append(values[23] + "\001");// CANCEL_DATE
							tmp1.append(values[24] + "\001");// CANCEL_TYPE

							tmp1.append(values[25] + "\001");// CANCEL_REASON1
							tmp1.append(values[26].replace("\001", "") + "\001");// CANCEL_REMARKS

							if (!values[22].equals("")) {
								if (!values[23].equals("")) {

									tmp1.append("disconnected" + "\001");
								} else {
									tmp1.append("installed" + "\001");
								}
							} else {
								if (!values[23].equals("")) {
									tmp1.append("cancelled" + "\001");
								} else {
									tmp1.append("submitted" + "\001");
								}
							} // ORDERSTATUS
							tmp1.append("" + "\001");// SEL_DUE_DATE
							tmp1.append("" + "\001");// ONT
							tmp1.append("" + "\001");// SELF_INSTALL

							tmp1.append("" + "\001");// eONT_Flag
							tmp1.append("" + "\001");// SI_Offered
							// System.out.println(values[32]);

							bw.write(tmp1.toString());
							bw.newLine();
							bw.flush();

						}
						reader.close();
						writer.close();
						String hdfs_upload = properties.getProperty("hdfs_upload",
								"/user/hadoop/Verizon");
						src = new Path(saveFile + "orderstatus_verizon_"
								+ Timestamp + ".txt");
						dst = new Path(hdfs_upload);

						hdfs.copyFromLocalFile(src, dst);
						System.out.println("Upload " + saveFile
								+ "orderstatus_verizon_" + Timestamp + ".txt"
								+ "  to  " + conf.get("fs.defaultFS"));
					} 
						
					

				}
				else{
				if (timenow >= starttime && timenow <= endtime) {
					gflag=1;
					System.out.println("start to download file from "
							+ starttime);
					System.out.println("downloading    " + filename);
					sf.download(".", filename, saveFile + filename, sftp);
					System.out.println("store in " + saveFile + filename);

					File f1 = new File(saveFile + filename);
					File f2 = new File(saveFile + "orderstatus_verizon_"
							+ Timestamp + ".txt");
					System.out.println(saveFile + "orderstatus_verizon_"
							+ Timestamp + ".txt");
					String[] values = new String[100];
					FileReader reader = new FileReader(f1);
					FileWriter writer = new FileWriter(f2, true);
					BufferedReader br = new BufferedReader(reader);
					BufferedWriter bw = new BufferedWriter(writer);
					String line = "";
					String keyline = br.readLine();
					String[] keys = keyline.split("~");

					while ((line = br.readLine()) != null) {
						line = line + "\001";

						values = line.split("\\~");// 文件时间以后在弄
						// System.out.println(values[3]);
						// / for(int i=0;i<values.length;i++){
						// System.out.println(values[i]);
						// }//main
						StringBuffer tmp1 = new StringBuffer("");
						String aaa = Timestamp.substring(0, 4);
						// System.out.println(aaa);
						String bbb = Timestamp.substring(4, 6);
						// System.out.println(bbb);
						String ccc = Timestamp.substring(6, 8);
						// System.out.println(ccc);

						tmp1.append(aaa + "-" + bbb + "-" + ccc + " 12:00:00"
								+ "\001");// DateTime
						tmp1.append(values[0] + "\001");// CHANNEL
						tmp1.append(values[1] + "\001");// VENDOR
						tmp1.append("verizon" + "\001");// PARTNER
						tmp1.append(values[2] + "\001");// REP_LNAME
						tmp1.append(values[3] + "\001");// REP_FNAME
						tmp1.append(values[4] + "\001");// STORE_ID
						tmp1.append(values[5] + "\001");// SOURCE
						tmp1.append(values[6] + "\001");// BTN
						tmp1.append(values[7] + "\001");// CBR
						tmp1.append(values[8] + "\001");// CUST_LNAME
						tmp1.append(values[9] + "\001");// CUST_FNAME
						tmp1.append(values[10] + "\001");// ADDRESS
						tmp1.append(values[11] + "\001");// CITY
						tmp1.append(values[12] + "\001");// STATE
						tmp1.append(values[13] + "\001");// ZIP
						tmp1.append(values[14] + "\001");// EMAIL
						tmp1.append(values[15] + "\001");// SALES_DATE
						tmp1.append(values[16] + "\001");// ORDER_NBR
						tmp1.append(values[17] + "\001");// MON
						tmp1.append(values[18] + "\001");// USOC
						tmp1.append(values[19] + "\001");// PRODUCT_GROUP
						tmp1.append(values[20] + "\001");// PRODUCT_ID
						tmp1.append(values[21] + "\001");// PRODUCT
						tmp1.append("" + "\001");// QTY
						tmp1.append(values[22] + "\001");// INSTALL_DATE
						tmp1.append(values[23] + "\001");// CANCEL_DATE
						tmp1.append(values[24] + "\001");// CANCEL_TYPE

						tmp1.append(values[25] + "\001");// CANCEL_REASON1
						tmp1.append(values[26].replace("\001", "") + "\001");// CANCEL_REMARKS

						if (!values[22].equals("")) {
							if (!values[23].equals("")) {

								tmp1.append("disconnected" + "\001");
							} else {
								tmp1.append("installed" + "\001");
							}
						} else {
							if (!values[23].equals("")) {
								tmp1.append("cancelled" + "\001");
							} else {
								tmp1.append("submitted" + "\001");
							}
						} // ORDERSTATUS
						tmp1.append("" + "\001");// SEL_DUE_DATE
						tmp1.append("" + "\001");// ONT
						tmp1.append("" + "\001");// SELF_INSTALL

						tmp1.append("" + "\001");// eONT_Flag
						tmp1.append("" + "\001");// SI_Offered
						// System.out.println(values[32]);

						bw.write(tmp1.toString());
						bw.newLine();
						bw.flush();

					}
					reader.close();
					writer.close();
					String hdfs_upload = properties.getProperty("hdfs_upload",
							"/user/hadoop/Verizon");
					src = new Path(saveFile + "orderstatus_verizon_"
							+ Timestamp + ".txt");
					dst = new Path(hdfs_upload);

					hdfs.copyFromLocalFile(src, dst);
					System.out.println("Upload " + saveFile
							+ "orderstatus_verizon_" + Timestamp + ".txt"
							+ "  to  " + conf.get("fs.defaultFS"));

				
				}
				
				}

			}

		}
		if (flag.equals("true")&&gflag==0) {
			System.out.println("file " +"ORDERSTATUS_INETACQ_DTV_" +starttime+".txt"

			 + " do not exist");
			System.exit(1);

	

	}
		// 上传文件到hdfs
		// 删除临时文件

		System.out.println("all files have been put to hdfs,have fun! ");
		System.exit(0);

	}

	public static void AddErrorCode(BasicDBObject document, String Error_Code,
			DB db) {
		DBCollection Error = db.getCollection("Error");
		System.out.println("insert error");
		// System.out.println(values[i]);
		if (Error_Code.equals("EU_0001")) {
			document.put("ERROR_TYPE", "wrong_update");
			document.put("ERROR_CODE", "EU_0001");
			document.put("ERROR_MESSAGE",
					"Disconnected cannot update submitted");
			document.put("ERROR_TIME", document.get("DateTime"));

		}
		if (Error_Code.equals("EU_0002")) {
			document.put("ERROR_TYPE", "Update_terminal");
			document.put("ERROR_CODE", "EU_0002");
			document.put("ERROR_MESSAGE", "Submitted cannot update canceled");
			document.put("ERROR_TIME", document.get("DateTime"));

		}
		if (Error_Code.equals("EU_0003")) {
			document.put("ERROR_TYPE", "Update_terminal");
			document.put("ERROR_CODE", "EU_0003");
			document.put("ERROR_MESSAGE", "installed cannot update canceled");
			document.put("ERROR_TIME", document.get("DateTime"));

		}
		if (Error_Code.equals("EU_0004")) {
			document.put("ERROR_TYPE", "Update_terminal");
			document.put("ERROR_CODE", "EU_0004");
			document.put("ERROR_MESSAGE", "installed cannot update canceled");
			document.put("ERROR_TIME", document.get("DateTime"));

		}
		if (Error_Code.equals("EU_0005")) {
			document.put("ERROR_TYPE", "Update_terminal");
			document.put("ERROR_CODE", "EU_0005");
			document.put("ERROR_MESSAGE", "Submitted cannot update installed");
			document.put("ERROR_TIME", document.get("DateTime"));

		}
		if (Error_Code.equals("EU_0006")) {
			document.put("ERROR_TYPE", "Update_terminal");
			document.put("ERROR_CODE", "EU_0006");
			document.put("ERROR_MESSAGE", "canceled cannot update installed");
			document.put("ERROR_TIME", document.get("DateTime"));

		}
		if (Error_Code.equals("EU_0007")) {
			document.put("ERROR_TYPE", "Update_terminal");
			document.put("ERROR_CODE", "EU_0007");
			document.put("ERROR_MESSAGE",
					"Submitted cannot update disconnected");
			document.put("ERROR_TIME", document.get("DateTime"));

		}
		if (Error_Code.equals("EU_0008")) {
			document.put("ERROR_TYPE", "Update_terminal");
			document.put("ERROR_CODE", "EU_0008");
			document.put("ERROR_MESSAGE", "canceled cannot update disconnected");
			document.put("ERROR_TIME", document.get("DateTime"));

		}
		if (Error_Code.equals("EU_0009")) {
			document.put("ERROR_TYPE", "Update_terminal");
			document.put("ERROR_CODE", "EU_0009");
			document.put("ERROR_MESSAGE",
					"installed cannot update disconnected");
			document.put("ERROR_TIME", document.get("DateTime"));

		}

		Error.insert(document);

	}

	public static void AddException(BasicDBObject document, String Exception,
			DB db) {
		DBCollection Exception1 = db.getCollection("Exception");
		System.out.println("insert Exception");
		if (Exception.equals("Ex_0001")) {
			document.put("EXCEPTION_TYPE", "Threshold_exceeding");
			document.put("EXCEPTION_CODE", "Ex_0001");
			document.put("EXCEPTION_MESSAGE",
					"Updating from submitted to canceled exceeds the threshold");
			document.put("EXCEPTION_TIME", document.get("DateTime"));

		}
		if (Exception.equals("Ex_0002")) {
			document.put("EXCEPTION_TYPE", "Threshold_exceeding");
			document.put("EXCEPTION_CODE", "Ex_0002");
			document.put("EXCEPTION_MESSAGE",
					"Updating from submitted to installed exceeds the threshold");
			document.put("EXCEPTION_TIME", document.get("DateTime"));

		}
		if (Exception.equals("Ex_0003")) {
			document.put("EXCEPTION_TYPE", "Threshold_exceeding");
			document.put("EXCEPTION_CODE", "Ex_0003");
			document.put("EXCEPTION_MESSAGE",
					"Updating from installed to disconnected exceeds the threshold");
			document.put("EXCEPTION_TIME", document.get("DateTime"));

		}

		Exception1.insert(document);

	}

	public static int getdate(String Datetime) {
		String day = Datetime.substring(0, 4) + Datetime.substring(5, 7)
				+ Datetime.substring(8, 10);

		return Integer.parseInt(day);

	}

	public static int caculatedate(int from, int to) throws ParseException {
		SimpleDateFormat df = new SimpleDateFormat("yyyyMMdd");
		Date fromdate = null;
		Date todate = null;

		fromdate = df.parse(String.valueOf(from));
		todate = df.parse(String.valueOf(to));
		int day = (int) ((todate.getTime() - fromdate.getTime()) / (24 * 60 * 60 * 1000));

		return day;

	}

	public static void hdfs2mongo(int from, int to) throws IOException {

		Configuration conf = new Configuration();
		FileInputStream fis = new FileInputStream(baseconf);

		Properties properties = new Properties();
		properties.load(fis);
		String mongodbServerIP = properties.getProperty("mongodb_server_ip",
				"10.214.0.144");
		String mongodb_port = properties.getProperty("mongodb_port", "27017");
		String hdfs_server = properties.getProperty("hdfs_server",
				"hdfs://10.214.0.144:9000");
		String database = properties.getProperty("database", "test2");
		String hdfs_download = properties
				.getProperty("hdfs_download",
						"hdfs://10.214.0.144:9000/user/hadoop/Verizon/OrderStatusUpdate/day=");
		conf.set("fs.defaultFS", hdfs_server);

		Mongo mongo = new Mongo(mongodbServerIP, 27017);// 链接配置
		// 连接名为wiretap的数据库，假如数据库不存在的话，mongodb会自动建立
		DB db = mongo.getDB(database);
		System.out.println("Get MongoDB ");
		// Get collection from MongoDB, database named "yourDB"
		// 从Mongodb中获得名为yourColleection的数据集合，如果该数据集合不存在，Mongodb会为其新建立
		DBCollection collection = db.getCollection("Log");
		DBCollection Consolidated = db.getCollection("Consolidated");

		String[] keys = new String[100];
		String[] values = new String[100];
		// int b=0;
		String line = "";
		String keyline = "";

		for (int k = from; k <= to; k++) {

			String remotefile = hdfs_download + Integer.toString(k)
					+ "/000000_0";// hdfs文件
			FileSystem fs = FileSystem.get(URI.create(remotefile), conf);

			Path path = new Path(remotefile);
			if (k / 100 % 100 == 12) {
				k = k + 8700;
			}
			if (k % 100 == 32) {
				k = k + 67;
			}
			if (!fs.exists(path)) {
				// System.out.println("file "+remotefile+"does not exist");
				continue;
			}
			FSDataInputStream in = fs.open(path);

			BufferedReader br = new BufferedReader(new InputStreamReader(in));

			System.out.print("get file" + Integer.toString(k));

			try {

				while ((line = br.readLine()) != null) {
					line = line + "~";
					values = line.split("\001");

					BasicDBObject document = new BasicDBObject();

					document.put("DateTime", values[0]);

					document.put("CHANNEL", values[1]);
					document.put("VENDOR", values[2]);
					document.put("PARTNER", "verizon");
					document.put("REP_LNAME", values[4]);
					document.put("REP_FNAME", values[5]);
					document.put("STORE_ID", values[6]);
					document.put("SOURCE", values[7]);
					document.put("BTN", values[8]);
					document.put("CBR", values[9]);
					document.put("CUST_LNAME", values[10]);
					document.put("CUST_FNAME", values[11]);
					document.put("ADDRESS", values[12]);
					document.put("CITY", values[13]);
					document.put("STATE", values[14]);
					document.put("ZIP", values[15]);
					document.put("EMAIL", values[16]);
					document.put("SALES_DATE", values[17]);
					document.put("ORDER_NBR", values[18]);
					document.put("MON", values[19]);
					document.put("USOC", values[20]);
					document.put("PRODUCT_GROUP", values[21]);
					document.put("PRODUCT_ID", values[22]);
					document.put("PRODUCT", values[23]);
					document.put("QTY", values[24]);
					document.put("INSTALL_DATE", values[25]);
					document.put("CANCEL_DATE", values[26]);
					document.put("CANCEL_TYPE", values[27]);
					document.put("CANCEL_REASON1", values[28]);
					document.put("CANCEL_REMARKS", values[29]);
					document.put("others", "");

					document.put("PROVIDER_ID", "0000");
					document.put("PROVIDER_NAME", "Direct TV");
					document.put("PARTNER_ID", "");
					// document.put("PARTNER_NAME", "verizon");

					document.put("ORDERSTATUS", values[30]);

					document.put("SOURCE_API", "");
					document.put("ORDER_DATE", "");
					document.put("OFFER_ID", "");

					document.put("SEL_DUE_DATE", values[31]);
					document.put("ONT", values[32]);
					document.put("SELF_INSTALL", values[33]);
					document.put("eONT_Flag", values[34]);
					document.put("SI_Offered", values[35]);

					// document.put(keys[i],values[i]);

					// 将新建立的document保存到collection中去

					// 做查询

					BasicDBObject obj = new BasicDBObject();
					obj.put("ORDER_NBR", values[18]);
					obj.put("MON", values[19]);
					obj.put("PRODUCT_ID", values[22]);
					obj.put("IS_CURRENT", 1);

					DBObject cursor = collection.findOne(obj);

					if (cursor != null) {// 如果找得到

						String ORDERSTATUS = (String) cursor.get("ORDERSTATUS");
						String timestamp = (String) cursor.get("DateTime"); // 时间相减
						// System.out.println("find " + ORDERSTATUS);
						// System.out.println(ORDERSTATUS+""+values[30]);
						if (ORDERSTATUS.equals(values[30]))
						// cursor.close();
						{

							// 数据重复了
							// System.out.println("same");
						} else {
							System.out.println(ORDERSTATUS);
							if (ORDERSTATUS.equals("submitted")) {

								if (values[30].equals("canceled")) { // submit-cancel

									System.out.println("submit-cancel");
									// System.out.println(values[18]);

									cursor.put("IS_CURRENT", 0);
									cursor.put("STATUS_EXPIRE_TIME",
											String.valueOf(getdate(values[0])));
									collection.update(
											new BasicDBObject("_id",
													cursor.get("_id")), cursor)
											.getN();
									document.put("IS_CURRENT", 1);
									document.put("STATUS_EFFECTIVE_TIME",
											String.valueOf(getdate(timestamp)));

									document.put("STATUS_EXPIRE_TIME", "");
									int date = caculatedate(getdate(timestamp),
											getdate(values[0]));
									if (date >= 5) {
										AddException(document, "Ex_0001", db);

									}
									document.put("DAYS_SINCE_LAST_OS_CHANGED",
											date);
									document.put("DAYS_SINCE_ORDER_WAS_PLACED",
											"");
									collection.insert(document);

									document.remove("STATUS_EXPIRE_TIME");
									document.remove("IS_CURRENT");
									document.put("PROVIDER_CONFIRMATION_ID", "");
									Consolidated.insert(document);

									Consolidated.remove(new BasicDBObject(
											"_id", cursor.get("_id")));

								}
								if (values[30].equals("installed")) {// submit-install
									System.out.println("submit-install");
									// System.out.println(values[18]);
									cursor.put("IS_CURRENT", 0);
									cursor.put("STATUS_EXPIRE_TIME",
											String.valueOf(getdate(values[0])));
									collection.update(
											new BasicDBObject("_id",
													cursor.get("_id")), cursor)
											.getN();
									document.put("IS_CURRENT", 1);
									document.put("STATUS_EFFECTIVE_TIME",
											String.valueOf(getdate(timestamp)));
									document.put("STATUS_EXPIRE_TIME", "");
									int date = caculatedate(getdate(timestamp),
											getdate(values[0]));
									if (date >= 5) {
										AddException(document, "Ex_0002", db);

									}
									document.put("DAYS_SINCE_LAST_OS_CHANGED",
											date);
									document.put("DAYS_SINCE_ORDER_WAS_PLACED",
											"");
									collection.insert(document);

									document.remove("STATUS_EXPIRE_TIME");
									document.remove("IS_CURRENT");
									document.put("PROVIDER_CONFIRMATION_ID", "");
									Consolidated.insert(document);
									Consolidated.remove(new BasicDBObject(
											"_id", cursor.get("_id")));

								}

								if (values[30].equals("disconnected")) {
									AddErrorCode(document, "EU_0001", db);

								}
							}

							if (ORDERSTATUS.equals("canceled")) {
								if (values[30].equals("submitted")) {
									AddErrorCode(document, "EU_0002", db);

								}
								if (values[30].equals("installed")) {
									AddErrorCode(document, "EU_0003", db);

								}
								if (values[30].equals("disconnected")) {
									AddErrorCode(document, "EU_0004", db);

								}

							}
							if (ORDERSTATUS.equals("installed")) {

								if (values[30].equals("submitted")) {
									AddErrorCode(document, "EU_0005", db);

								}
								if (values[30].equals("canceled")) {
									AddErrorCode(document, "EU_0006", db);

								}
								if (values[30].equals("disconnected")) { // install-disconnected
									System.out.println("install-disconnected");
									// System.out.println(values[18]);
									cursor.put("IS_CURRENT", 0);
									cursor.put("STATUS_EXPIRE_TIME",
											String.valueOf(getdate(values[0])));
									collection.update(
											new BasicDBObject("_id",
													cursor.get("_id")), cursor)
											.getN();
									document.put("IS_CURRENT", 1);
									document.put("STATUS_EFFECTIVE_TIME",
											String.valueOf(getdate(timestamp)));
									document.put("STATUS_EXPIRE_TIME", "");
									int date = caculatedate(getdate(timestamp),
											getdate(values[0]));
									if (date >= 5) {
										AddException(document, "Ex_0003", db);

									}
									document.put("DAYS_SINCE_LAST_OS_CHANGED",
											date);
									BasicDBObject tmp = new BasicDBObject();
									tmp.put("ORDER_NBR", values[18]);
									tmp.put("MON", values[19]);
									tmp.put("PRODUCT_ID", values[22]);
									tmp.put("submitted", values[30]);

									DBObject cursor1 = collection.findOne(tmp);

									if (cursor1 != null) {// 如果找得到
										String timestamp1 = (String) cursor1
												.get("DateTime"); // 时间相减
										document.put(
												"DAYS_SINCE_ORDER_WAS_PLACED",
												caculatedate(
														getdate(timestamp1),
														getdate(values[0])));

									} else {

										document.put(
												"DAYS_SINCE_ORDER_WAS_PLACED",
												"");// 待解决

									}

									collection.insert(document);

									document.remove("STATUS_EXPIRE_TIME");
									document.remove("IS_CURRENT");
									document.put("PROVIDER_CONFIRMATION_ID", "");
									Consolidated.insert(document);
									Consolidated.remove(new BasicDBObject(
											"_id", cursor.get("_id")));

								}

							}

							if (ORDERSTATUS.equals("disconnected")) {
								if (values[30].equals("submitted")) {
									AddErrorCode(document, "EU_0007", db);

								}
								if (values[30].equals("canceled")) {
									AddErrorCode(document, "EU_0008", db);

								}
								if (values[30].equals("installed")) {
									AddErrorCode(document, "EU_0009", db);

								}

							}

							// 存在旧数据

						}
					}

					else {
						// 这是新数据
						// System.out.println("new");

						if (true) {

							document.put("IS_CURRENT", 1);
							document.put("STATUS_EFFECTIVE_TIME",
									String.valueOf(getdate(values[0])));
							document.put("STATUS_EXPIRE_TIME", "");

							document.put("DAYS_SINCE_LAST_OS_CHANGED", 0);
							document.put("DAYS_SINCE_ORDER_WAS_PLACED", "");
							collection.insert(document);

							document.remove("STATUS_EXPIRE_TIME");
							document.remove("IS_CURRENT");
							document.put("PROVIDER_CONFIRMATION_ID", "");
							Consolidated.insert(document);
						}
					}

					// document.put("islatest",1);

					// collection.insert(document);

				}
				System.out.println("put the " + remotefile
						+ " data into MondoDB");

				System.out.println("collection  log");

				// keyline =br.readLine();
				// keys=keyline.split(",");

				fs.close();

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

		System.out.println("over");
		fis.close();
		System.exit(0);
	}

	public static int filestarttime;
	static String baseconf;
	static String flag;

	public static void main(String[] args) throws IOException, SftpException,
			ParseException {

		String Starttime = "";
		// 后期由系统产生
		int endtime = 20140115;
		int starttime = 20140115;
		if (args.length == 0) {// 没输出时间，默认系统时间
			Starttime = "23:40:00";
			filestarttime = 20140115;

			System.out
					.println("usage:Ftp2Hdfs_fat.jar 15:00:00 20140113 20140113\n");
			System.out
					.println("The system will get ftp data at 15:00:00 everyday and get data from time"
							+ filestarttime);
		}

		else {
			baseconf = args[3];
			flag = args[4];
			System.out.println(baseconf);
			if (baseconf.equals("")) {
				baseconf = "baseconf.properties";
			}
			starttime = Integer.parseInt(args[1]);
			endtime = Integer.parseInt(args[2]);
			Starttime = args[0];
			if (Starttime.equals("ftp")) {
				getftpdata(starttime, endtime);

			}
			if (Starttime.equals("hdfs")) {
				hdfs2mongo(starttime, endtime);

			}

			// filestarttime=Integer.parseInt(args[1]);
			System.out.println("The system will get ftp data at " + Starttime
					+ " everyday and get data from time" + starttime + " to "
					+ endtime);

		}
		// getftpdata(starttime,endtime);
		// getftpdata(20140122,20140122);
		// hdfs2mongo(20131231, 20140102);

		// 一天的毫秒数

		// 以每24小时执行一次

	}
}
