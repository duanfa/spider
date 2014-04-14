package hadoop.hbase;

import hbase.OperateTable;

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

public class DoubanOperater {
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

	// 添加一条数据
	public static boolean addRow(String tableName, String row, String columnFamily, String column, String value) throws Exception {
		try {
			HTable table = new HTable(conf, tableName);
			Put put = new Put(Bytes.toBytes(row));
			// 参数出分别：列族、列、值
			put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}

	/**
	 * 
	 * @param category
	 * @param id
	 * @param href
	 * @return 0 success,1 exist ,2 save exception
	 * @throws Exception
	 */
	public static int saveHref(String category, String id, String href){
		try {
			Result result = getRow(category + "_href", id);
			// 输出结果
			for (KeyValue rowKV : result.raw()) {
				if(href.equals(new String(rowKV.getQualifier()))){
					return 1;
				}
			}
			if(addRow(category + "_href", id, "href", href, "")){
				return 0;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 2;
	}

	// get row
	public static Result getRow(String tableName, String row) throws Exception {
		HTable table = new HTable(conf, tableName);
		Get get = new Get(Bytes.toBytes(row));
		Result result = table.get(get);

		return result;
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
	public static void main(String[] args) {
		try {
			String[] columnFamilys = { "href" };
			//
			// // 第二步：向数据表的添加数据
			// // 添加第一行数据
			// DoubanOperater.addRow(tableName, "47538699", "href",
			// "http://movie.douban.com/people/47538699/","");
			// DoubanOperater.addRow(tableName, "47538699", "href",
			// "http://movie.douban.com/people/47538699/other","");
			// System.out.println("获取所有数据");
			// DoubanOperater.getAllRows(tableName);
			// String[] rows = { "47538699" };
			// OperateTable.delMultiRows(tableName, rows);
		} catch (Exception err) {
			err.printStackTrace();
		}
	}
}