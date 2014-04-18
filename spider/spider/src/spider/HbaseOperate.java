package spider;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseOperate {
	// 声明静态配置
	private static Configuration conf = null;
	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "server61");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}

	// 创建数据库表
	public static void createTable(String tableName, String[] columnFamilys) throws Exception {
		// 新建一个数据库管理员
		HBaseAdmin hAdmin = new HBaseAdmin(conf);

		if (hAdmin.tableExists(tableName)) {
			System.out.println("表已经存在");
			System.exit(0);
		} else {
			// 新建一个 scores 表的描述
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			// 在描述里添加列族
			for (String columnFamily : columnFamilys) {
				tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			}
			// 根据配置好的描述建表
			hAdmin.createTable(tableDesc);
			System.out.println("创建表成功");
		}
	}

	// 删除数据库表
	public static void deleteTable(String tableName) throws Exception {
		// 新建一个数据库管理员
		HBaseAdmin hAdmin = new HBaseAdmin(conf);

		if (hAdmin.tableExists(tableName)) {
			// 关闭一个表
			hAdmin.disableTable(tableName);
			// 删除一个表
			hAdmin.deleteTable(tableName);
			System.out.println("删除表成功");

		} else {
			System.out.println("删除的表不存在");
			System.exit(0);
		}
	}

	// 添加一条数据
	public static void addRow(String tableName, String row, String columnFamily, String column, String value) throws Exception {
		HTable table = new HTable(conf, tableName);
		Put put = new Put(Bytes.toBytes(row));
		// 参数出分别：列族、列、值
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
		table.put(put);
	}

	// 删除一条数据
	public static void delRow(String tableName, String row) throws Exception {
		HTable table = new HTable(conf, tableName);
		Delete del = new Delete(Bytes.toBytes(row));
		table.delete(del);
	}

	// 删除多条数据
	public static void delMultiRows(String tableName, String[] rows) throws Exception {
		HTable table = new HTable(conf, tableName);
		List<Delete> list = new ArrayList<Delete>();

		for (String row : rows) {
			Delete del = new Delete(Bytes.toBytes(row));
			list.add(del);
		}

		table.delete(list);
	}

	// get row
	public static Result getRow(String tableName, String row) throws Exception {
		HTable table = new HTable(conf, tableName);
		Get get = new Get(Bytes.toBytes(row));
		Result result = table.get(get);
		return result;
		// 输出结果
//		for (KeyValue rowKV : result.raw()) {
//			System.out.print("Row Name: " + new String(rowKV.getRow()) + " ");
//			System.out.print("Timestamp: " + rowKV.getTimestamp() + " ");
//			System.out.print("column Family: " + new String(rowKV.getFamily()) + " ");
//			System.out.print("Row Name:  " + new String(rowKV.getQualifier()) + " ");
//			System.out.println("Value: " + new String(rowKV.getValue()) + " ");
//		}
	}

	// get all records
	public static void getAllRows(String tableName) throws Exception {
		HTable table = new HTable(conf, tableName);
		Scan scan = new Scan();
		ResultScanner results = table.getScanner(scan);
		// 输出结果
		for (Result result : results) {
			for (KeyValue rowKV : result.raw()) {
				System.out.print("Row Name: " + new String(rowKV.getRow()) + " ");
				System.out.print("Timestamp: " + rowKV.getTimestamp() + " ");
				System.out.print("column Family: " + new String(rowKV.getFamily()) + " ");
				System.out.print("Row Name:  " + new String(rowKV.getQualifier()) + " ");
				System.out.println("Value: " + new String(rowKV.getValue()) + " ");
			}
		}
	}

	// main
	public static void main(String[] args) throws Exception {
		String url = "http://movie.douban.com/doulist/583122/?start=100&amp;filter=";
		System.out.println(isRead(url));
	}

	private static void insertFileToHbase() {
		try {
			String tableName = "people";
			// 第一步：创建数据库表：“users2”
			String[] columnFamilys = { "url"};
			HbaseOperate.createTable(tableName, columnFamilys);
			tableName = "subject";
			String[] columnFamilys2 = { "url"};
			HbaseOperate.createTable(tableName, columnFamilys2);
			tableName = "review";
			String[] columnFamilys3 = { "url"};
			HbaseOperate.createTable(tableName, columnFamilys3);
			tableName = "celebrity";
			String[] columnFamilys4 = { "url"};
			HbaseOperate.createTable(tableName, columnFamilys4);
			tableName = "doulist";
			String[] columnFamilys5 = { "url"};
			HbaseOperate.createTable(tableName, columnFamilys5);

			// 第二步：向数据表的添加数据
			// 添加第一行数据

			//HbaseOperate.addRow(tableName, "tht", "info", "age", "20");
			
			FileInputStream fis = new FileInputStream(Constant.otherUrlFilePath);
			BufferedReader br = new BufferedReader(new InputStreamReader(fis));
			String url = "";
			int index = 0;
			while((url=br.readLine())!=null){
				System.out.println(url+"---------line-----:"+index++);
				try {
					saveUrl(url);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			br.close();
		} catch (Exception err) {
			err.printStackTrace();
		}
	}

	static boolean isRead(String url)  {

		int i = 0;
		String split = "";
		String[] urls = url.split("/");
		Result result= null;
		for (String s : urls) {
			split = urls[i++];
			if ("people".equals(split)||"subject".equals(split)||"review".equals(split)||"celebrity".equals(split)||"group".equals(split)||"doulist".equals(split)) {
				try {
					result= getRow(split, urls[i]);
					break;
				} catch (Exception e) {
					e.printStackTrace();
					return false;
				}
			}
		}
		if(result==null){
			return false;
		}
		for (KeyValue rowKV : result.raw()) {
			if(url.equals(new String(rowKV.getQualifier()))){
				return true;
			}
		}
		return false;
	}
	static void saveUrl(String url) throws Exception {
		int i = 0;
		String split = "";
		String[] urls = url.split("/");
		for (String s : urls) {
			split = urls[i++];
			if ("people".equals(split)||"subject".equals(split)||"review".equals(split)||"celebrity".equals(split)||"group".equals(split)||"doulist".equals(split)) {
				HbaseOperate.addRow(split, urls[i], "url", url, "");
				 return ;
			}
		}
		saveOtherUrl(url);
		//System.out.println("other url:"+ url);
	}
	
	static FileOutputStream downloadFos = null;
	static BufferedWriter downloadBw = null;
	private static void saveOtherUrl(String url){
		try {
			if(downloadBw==null){
				downloadFos = new FileOutputStream(Constant.otherUrlFilePath,true);
				downloadBw = new BufferedWriter(new OutputStreamWriter(downloadFos));
			}
			downloadBw.newLine();
			downloadBw.write(url);
		} catch (IOException e) {
			e.printStackTrace();
			try {
				downloadBw.close();
				downloadFos = new FileOutputStream(Constant.downloadUrlFilePath,true);
				downloadBw = new BufferedWriter(new OutputStreamWriter(downloadFos));
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}
}