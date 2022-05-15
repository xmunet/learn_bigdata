
package com.naxu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneTrafficCount {
	
//    static {
//        try {
//            // 设置 HADOOP_HOME 环境变量
//            System.setProperty("hadoop.home.dir", "C:/NAXU/Tools/JAVA/hadoop-2.8.3.tar/hadoop-2.8.3/");
//            // 日志初始化
//            BasicConfigurator.configure();
//            // 加载库文件
//            System.load("C:/NAXU/Tools/JAVA/hadoop-2.8.3.tar/hadoop-2.8.3/bin/hadoop.dll");
//        } catch (UnsatisfiedLinkError e) {
//            System.err.println("Native code library failed to load.\n" + e);
//            System.exit(1);
//        }
//    }
	
	public static class FlowMapper extends Mapper<Object, Text, Text, FlowBean>{
		
		private final Text phone = new Text(); //定义phone key
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString(); //将文本文件按行读取
			String[] lineElements = line.split("\t");  //将行拆分为elements
			int elementCount = lineElements.length;    //行包含的elements的个数
			phone.set(lineElements[1]);                //将phone number 赋予 key中，类型为Text
			int up = Integer.parseInt(lineElements[elementCount-3]);
			int down = Integer.parseInt(lineElements[elementCount-2]);
			
			/*
			* 创建新的bean(up,down)
			*/
			FlowBean mybean = new FlowBean(up,down);          //创建map value 的值,类型为FlowBean
			/*
			* map输出kv 为：phone=>bean(up,down)
			*/
			context.write(phone, mybean);
		}
		
	}
	
	public static class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		
		public void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
			int totalUp=0;
			int totalDown=0;
			int total=0;
			
			FlowBean mybean = new FlowBean();
			
			//从values 中获取一队列beans ， 对这一列beans做计算
			
			for (FlowBean val:values) {   //遍历values枚举中的元素,每一个元素都是一个bean
				totalUp += val.getUp();    //获取 并 sum bean中的up值
				totalDown +=val.getDown();   //获取并sum bean中的down值
			}
			total = totalUp+totalDown;  
			/*
			* 重新组合成新的bean： key(phone)==> bean(sumup, sumdown, total)
			*/
			mybean.setSumUp(totalUp);
			mybean.setSumDown(totalDown);
			mybean.setTotal(total);
			/*
			* reducer输出kv:  phone==> bean(sumup, sumdown, total)
			*/
			context.write(key, mybean);
		}
	}
	
	public static class FlowBean implements Writable{
		/*
		* JavaBean 对象用来封装手机号的上行和下行流量，
		* 在map阶段KV
		* key (text)==> value<FlowBean> 
		* 手机号1 ==> bean(up, down)
		* 手机号1 ==>bean(up,down)
		* 手机号1 ==>bean(up,down)
		* 手机号2 ==>bean(up,down)
		* 在reducer阶段，
		* 输入时的KV: Key(text) => beans(iterator)
		* 手机号1 ==> bean1(up,down), bean2(up,down), bean3(up,down)  
		* 手机号2 ==> bean4(up,down)
		* 输出时的KV
		* Key<TEXT> ==> value<TEXT>
		* 手机号1 ==>sum(上行流量1,2,3)  sum(下行流量1,2,3)  sum(上行流量1,2,3+下行流量1,2,3)
		* 手机号2 ==>sum(上行流量4)      sum(下行流量4)	sum(上行流量4+下行流量4)
		*/
		private int upFlow;
		private int downFlow;
		private int sumUp;
		private int sumDown;
		private int total;
		
		public FlowBean() {
			
		}
		
		public FlowBean(int upFlow, int downFlow) {
			this.upFlow = upFlow;
			this.downFlow = downFlow;
		}
		
		
		public void setUp(int upFlow) {
			this.upFlow = upFlow;
		}
		public void setDown(int downFlow) {
			this.downFlow = downFlow;
		}
		public int getUp() {
			return this.upFlow;
		}
		public int getDown() {
			return this.downFlow;
		}
		public int getSumUp() {
			return this.sumUp;
		}
		public int getSumDown() {
			return this.sumDown;
		}
		public int getTotal() {
			return this.total;
		}
		public void setSumUp(int sumUp) {
			this.sumUp = sumUp;
		}
		public void setSumDown(int sumDown) {
			this.sumDown = sumDown;
		}		
		public void setTotal(int total) {
			this.total = total;
		}
		
		public void write(DataOutput out) throws IOException{
			/* 
			* 将状态写进二进制
			*/
			out.writeInt(this.downFlow);
			out.writeInt(this.upFlow);
		}
		
		public void readFields(DataInput in) throws IOException{
			/*
			* 读取二进制数据
			*/
			this.upFlow = in.readInt();
			this.downFlow = in.readInt();
		}
		
		public String toString() {
			return sumUp+ "\t" + sumDown+ "\t" + total;
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		args = new String[]{"E:\\temp\\HTTP_20130313143750.dat", "e:\\temp\\123"};
		
		System.out.println(args[0]);
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "flow");
        
        //设置Jar 包
        job.setJarByClass(PhoneTrafficCount.class);
        //关联mapper & reducer 
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);
        job.setNumReduceTasks(1);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        int status = job.waitForCompletion(true) ? 0 : 1;
        System.exit(status);
	}

}




 
