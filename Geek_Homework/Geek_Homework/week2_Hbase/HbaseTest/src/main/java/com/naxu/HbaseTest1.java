package com.naxu;


import org.apache.hadoop.conf.Configuration;
import java.util.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseTest1 {

  public static class HBaseOperation{
	  private static Connection conn = null;
	  private static Admin admin = null;
	  private static  ResultScanner resultScanner=null;
	  
	  //setup connection
	  public HBaseOperation() {
		  try {
			  
			  if(conn==null) {
				  Configuration conf = HBaseConfiguration.create();
				  //conf.set("hbase.zookeeper.quorum", "127.0.0.1");
				  conf.set("hbase.zookeeper.quorum", "emr-worker-2,emr-worker-1,emr-header-1");
			      conf.set("hbase.zookeeper.property.clientPort", "2181");
			      conn = ConnectionFactory.createConnection(conf);
			      admin = conn.getAdmin();
			  }
		  }
		  catch(Exception e) {
			  e.printStackTrace();
		  }
	  }
	  //close connection
	  public void close() {
		  	try {
			  if(conn !=null) {
				  conn.close();
			  }
		  
	  		}catch(IOException e) {
	  			e.printStackTrace();
	  		}
	  }
	  
	  //create table
	  public void createTable(String tableName, String[] columnFamilys) throws IOException {

		 
		  
		  try {
			  TableName tblName = TableName.valueOf(tableName);
			  if(admin.tableExists(tblName)) {
				  System.out.println("table has been existed.");
			  }else {
				  HTableDescriptor htd = new HTableDescriptor(tblName);
				  for(String columnFamily:columnFamilys) {
					  HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
					  htd.addFamily(hcd);
				  }
				  admin.createTable(htd);
				  System.out.println("create table successfully!");
			  }
			  
		  }catch(Exception e) {
			  e.printStackTrace();
		  }finally {
			  if(admin!=null) {
				  try {
					  admin.close();
				  }catch(IOException e) {
					  e.printStackTrace();
				  }
			  }
		  }
		  
	  }
	  
	   /*
	    * create name space
	    */
	    
	    public void createNameSapce(String ns) throws IOException {
	        //create name space descriptor
	        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(ns).build();
	        //create namespace
	        NamespaceDescriptor[] listNamespaceDes = admin.listNamespaceDescriptors();
	        boolean exists = false;
	        for(NamespaceDescriptor nsD: listNamespaceDes){
		       if (nsD.getName().equals(ns)){
		    	   exists=true;
		        	break;
		        }
  			}
	        
	        if(!exists){
	        	admin.createNamespace(namespaceDescriptor);
	        	System.out.println("create namespace:" + ns + " successful!");
	        	
	        }
	        else{
	        	System.out.println("namesapce has been existed");
	        }
	        
	        
	    }

	    /*
	     * delete name space
	     */
	    public void deleteNamespace(String ns) throws IOException {
	        admin.deleteNamespace(ns);
	        System.out.println("delete namespace successful!");
	    }
	  
	    
	    
	    public void insert(String tableName, int rowKey, String colFamily, String column, String value) {
	      
		  try {
		      TableName tblName = TableName.valueOf(tableName);
			  Put put = new Put(Bytes.toBytes(rowKey)); // insert row key
		      put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(column), Bytes.toBytes(value)); // insert column & value
		      conn.getTable(tblName).put(put);
		  }catch(IOException e){
			  e.printStackTrace();
		  }
	  }
	  
//	  public void insert(String tableName, int rowKey, String colFamily, String column, int value) {
//	      
//		  try {
//		      TableName tblName = TableName.valueOf(tableName);
//			  Put put = new Put(Bytes.toBytes(rowKey)); // insert row key
//		      put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes(column), Bytes.toBytes(value)); // insert column & value
//		      conn.getTable(tblName).put(put);
//		  }catch(IOException e){
//			  e.printStackTrace();
//		  }
//	  }
	  
	  /*create record
	   * 
	   */
	  public void insert(String tableName, int rowKey, String colFamily, String value) {
	      
		  try {
		      TableName tblName = TableName.valueOf(tableName);
			  Put put = new Put(Bytes.toBytes(rowKey)); // insert row key
		      put.addColumn(Bytes.toBytes(colFamily), null, Bytes.toBytes(value)); // insert column & value
		      conn.getTable(tblName).put(put);
		  }catch(IOException e){
			  e.printStackTrace();
		  }
	  }
	  
	  public List getColFamilys(String tableName) throws IOException {
		  Table table = conn.getTable(TableName.valueOf(tableName));
		  List<String> list=new ArrayList<>();
		  HTableDescriptor htd = table.getTableDescriptor();
		  for(HColumnDescriptor hcd:htd.getColumnFamilies()) {
			 // hcd.getNameAsString();
			  list.add(hcd.getNameAsString());
			  System.out.println(hcd.getNameAsString());
			  
		  }
		  return list;
		  
	  }
	  
	  /*
	   * print single record
	   */
	  public void getRowByRowkey(String tableName, int rowKey) throws IOException{
		
		  Table table = conn.getTable(TableName.valueOf(tableName));
		  Get get = new Get(Bytes.toBytes(rowKey));
	      if (!get.isCheckExistenceOnly()) {
	          Result result = table.get(get);
	          for (Cell cell : result.rawCells()) {
	              String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
	              String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
	              System.out.println("Data get success, colName: " + colName + ", value: " + value);
	          }
	      }
	  }
	  
	  /* print records
	   * 
	   */
	  private static void printScanResults() {
		  for(Result rs:resultScanner) {
			  
			  for(Cell cell:rs.listCells()) {
				  System.out.println(
					  "RowKey:"
					  +Bytes.toInt(rs.getRow())
					  +"Family:"
					  +Bytes.toString(CellUtil.cloneFamily(cell))
					  +"Qualifier:"
					  +Bytes.toString(CellUtil.cloneQualifier(cell))
					  +"Value:"
					  +Bytes.toString(CellUtil.cloneValue(cell))
				  );
			  }
			  
		  }
		  resultScanner.close();
	  }
	  
	  /*
	   * Print whole table
	   */
	  public void getAllRows(String tableName) throws IOException{
		  Table table = conn.getTable(TableName.valueOf(tableName));
		  Scan scan = new Scan();
		  resultScanner = table.getScanner(scan);
		  printScanResults();
	  }
	  
	  /*
	   * Print records by given column family
	   */
	  public void getRowsByColfamily(String tableName, String colFamily) throws IOException{
		  Table table = conn.getTable(TableName.valueOf(tableName));
		  Scan scan = new Scan();
		  scan.addFamily(Bytes.toBytes(colFamily));
		  resultScanner = table.getScanner(scan);
		  printScanResults();
	  }
	  
	  /*
	   * Print records by given column
	   */
	  
	  public void getRowsByCol(String tableName, String colFamily, String column) throws IOException{
		  Table table = conn.getTable(TableName.valueOf(tableName));
		  Scan scan = new Scan();
		  scan.addColumn(Bytes.toBytes(colFamily),Bytes.toBytes(column));
		  resultScanner = table.getScanner(scan);
		  printScanResults();
	  }
	  
	  /* 
	   * Delete records by given row key
	   */
	  
	  public void delRowByRowkey(String tableName, int rowKey) throws IOException{
		
		  Table table = conn.getTable(TableName.valueOf(tableName));
		  Delete delete = new Delete(Bytes.toBytes(rowKey));      
	      table.delete(delete);
	      System.out.println("Delete record Success");
	  }
	  
	 /*drop table
	  * 
	  */
	  
	  public void dropTable(String tableName) throws IOException{
		  TableName tblName = TableName.valueOf(tableName);
	      if (admin.tableExists(tblName)) {
	          admin.disableTable(tblName);
	          admin.deleteTable(tblName);
	          System.out.println("Table Delete Successful");
	      } else {
	          System.out.println("Table does not exist!");
	      }
	  }
  }

  
  
  public static void main(String[] args) throws IOException {
	  
	  
	  HBaseOperation ho = new HBaseOperation();
      String[] colFamilys = {"name","info","score"}; //define column family
      String tablename = "naxu:student";
      String colFamily = null;
      String colName = null;
      String[] names = {"Tom","Jerry","Jack","Rose","naxu"};
      String[] student_ids = {"2021000000001","2021000000002","2021000000003","2021000000004","G20220735030049"};
      String[] classes = {"1","1","2","2"};
      String[] understandings = {"75","85","80","60"};
      String[] programmings = {"82","67","80","61"};
      
      
      ho.createNameSapce("naxu");
      /*
       * create table 
       */
	  ho.createTable("naxu:student" ,colFamilys);	

	  
	  /*
	   * fill up the table
	   */
	  
	  colFamily = "name";
	  for(int i=0;i<=4;i++) {
		  ho.insert(tablename, i, colFamily,names[i]);  //insert value
	  }
	  
	  //insert values of info--> student_id'
	  colFamily="info";
	  colName = "student_id";
	  for(int i=0;i<=4;i++) {
		  ho.insert(tablename, i, colFamily,colName,student_ids[i]);  //insert value
	  }
	  
	  //insert values of info--> class'
	  colFamily="info";
	  colName = "class";
	  for(int i=0;i<=3;i++) {
		  ho.insert(tablename, i, colFamily,colName,classes[i]);  //insert value
	  }
	  
	  //insert values of score--> understanding'
	  colFamily="score";
	  colName = "understanding";
	  for(int i=0;i<=3;i++) {
		  ho.insert(tablename, i, colFamily,colName,understandings[i]);  //insert value
	  }
	  
	  //insert values of score--> programming'
	  colFamily="score";
	  colName = "programming";
	  for(int i=0;i<=3;i++) {
		  ho.insert(tablename, i, colFamily,colName,programmings[i]);  //insert value
	  }
	  
      /*query table
       * 
       */
	  System.out.println("List all records...........");
	  ho.getAllRows("naxu:student");
	  System.out.println("query by rowkey=4...........");
	  ho.getRowByRowkey("naxu:student",4);  
	  System.out.println("query by columnFamily=score...........");
	  ho.getRowsByColfamily("naxu:student","score");
	  System.out.println("query by column=understanding...........");
	  ho.getRowsByCol("naxu:student","score","understanding");  
	  
	  
	  /*delete row by row key
	   * 
	   */
	  
	  ho.delRowByRowkey("naxu:student", 1);
	  
	 
	  ho.dropTable("naxu:student");  //drop table
	 
	
	  ho.deleteNamespace("naxu"); //drop namesapce
	  
	  
	  ho.close();

  }
}
