package com.cry.bd;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

public class TestHBase {
	private static Configuration conf =null;

	static {
		conf = HBaseConfiguration.create();
	}
	public static void main(String[] args) throws Exception {
		TestHBase t = new TestHBase();
		t.f1("1_webpage");

	}

	public void f1(String tableName) throws Exception {
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TableName.valueOf(tableName));
		ResultScanner scanner = table.getScanner(new Scan());
			
		scanner.forEach(r->{
			byte[] row = r.getRow();
			System.out.println(new String(row));
			Cell[] rawCells = r.rawCells();
			for(Cell cell:rawCells) {
				System.out.println("");
				System.out.print(new String(CellUtil.cloneFamily(cell)));
				System.out.print("\t");
				System.out.print(new String(CellUtil.cloneQualifier(cell)));
				System.out.print("\t\t\t");
				System.out.print(new String(CellUtil.cloneValue(cell)));
			}

		});
		
	}
	public static void getAllRecord (String tableName) {
		try{
			HTable table = new HTable(conf, tableName);
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);
			for(Result r:ss){
				for(KeyValue kv : r.raw()){
					System.out.print(new String(kv.getRow()) + " ");
					System.out.print(new String(kv.getFamily()) + ":");
					System.out.print(new String(kv.getQualifier()) + " ");
					System.out.print(kv.getTimestamp() + " ");
					System.out.println(new String(kv.getValue()));
				}
			}
		} catch (IOException e){
			e.printStackTrace();
		}
	}
}
