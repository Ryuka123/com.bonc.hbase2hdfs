package com.bonc.hbaseMR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bonc.hbaseMR.util.OssFileNameTextOutputFormat;
import com.bonc.hbaseMR.util.TableInitializeInputFormat;


public class Main extends Configured implements Tool{

	private static final Logger LOG = LoggerFactory.getLogger(Main.class);
	
	private Configuration conf;
	
	@Override
	public void setConf(Configuration conf) {
		if(this.conf == null){
			this.conf = conf;
		}
	}

	@Override
	public Configuration getConf() {
		if(this.conf == null){
			return HBaseConfiguration.create();
		}
		return this.conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		String kvName = "";
		String outputPath = "";
		String jobName = "";
		String confPath = "";
		String hbaseTableName = "";
		String startTime = "null";
		String endTime = "null";
		String compressor = "txt";
		String tableName = "all";
		String confFilePath = "";
		String userName = "hjpt";
		
		if(args.length > 0){
			for(int i = 0; i<= args.length-1;i++){
				switch(args[i]){
				case "-kvName":
					kvName = args[i+1];
					break;
				case "-outputPath":
					outputPath = args[i+1];
					break;
				case "-jobName":
					jobName = args[i+1];
					break;
				case "-startTime":
					startTime = args[i+1];
					break;
				case "-endTime" :
					endTime = args[i+1];
					break;
				case "-confPath":
					confPath = args[i+1];
					break;
				case "-hbaseTableName":
					hbaseTableName = args[i+1];
					break;
				case "-compressor":
					compressor = args[i+1];
					break;
				case "-tableName" :
					tableName = args[i+1];
					break;
				case "-confFilePath" :
					confFilePath = args[i+1];
					break;
				case "-userName":
					userName = args[i+1];
					break;
				}
			}
			
			if(hbaseTableName.equals("") || kvName.equals("") || outputPath.equals("") || confPath.equals("") || jobName.equals("") || confFilePath.equals("")){
				LOG.error("请输入完整参数");
				System.exit(-1);
			}
			
			Path path = new Path(confPath);
			conf.addResource(path);
		}else{
			LOG.error("请输入参数");
			System.exit(-1);
		}
		
		conf.set("conf.startTime", startTime);
		conf.set("conf.endTime", endTime);
		conf.set("conf.kvName", kvName);
		conf.set("conf.tableName", tableName);
		conf.set("conf.confFilePath", confFilePath);
		conf.set("hbase2hdfs.user.name", userName);
		
		Path outPath = new Path(outputPath);
		
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath,true);
		}
		fs.close();
		
		Job job = Job.getInstance(conf, jobName);
		job.setJarByClass(Main.class);
		
		Scan scan = new Scan();
		scan.setMaxVersions(1);
		scan.setCacheBlocks(false);
		TableMapReduceUtil.initTableMapperJob(hbaseTableName, scan, BoncTableMapper.class, NullWritable.class, Text.class, job);
		
		job.setInputFormatClass(TableInitializeInputFormat.class);
		job.setNumReduceTasks(0);
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    FileOutputFormat.setOutputPath(job, outPath);
	    
	    switch(compressor){
	    case "gz":
	    	 FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
	    	 break;
	    case "deflate":
	    	 FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
	    	 break;
	    case "bz2":
	    	 FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
	    	 break;
	    case "lz4":
	    	 FileOutputFormat.setOutputCompressorClass(job, Lz4Codec.class);
	    	 break;
	    case "snappy":
	    	 FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
	    	 break;
	    }
		LazyOutputFormat.setOutputFormatClass(job, OssFileNameTextOutputFormat.class);
		
		
		return  job.waitForCompletion(true)? 0 : 1;
	}
	
	public static void main(String[] args) {
		try {
			ToolRunner.run(new Main(), args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

}
