package com.bonc.hbaseMR;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;


public class BoncTableMapper extends TableMapper<NullWritable, Text> {

	private String endFiledecollator;
	private String kvName = "";
	private String tableName = "";
	private long startTime = 0;
	private long endTime = Long.MAX_VALUE;
	private String rowKey = "";

	private String separator = System.getProperty("file.separator");

	private MultipleOutputs<NullWritable, Text> mos;
	private Map<String, ArrayList<HashMap<String, String>>> map = new HashMap<String, ArrayList<HashMap<String, String>>>();

	@Override
	protected void setup(Mapper<ImmutableBytesWritable, Result, NullWritable, Text>.Context context) throws IOException, InterruptedException {
		String confFileDecollator = "\\|";
		String confFilePath = context.getConfiguration().get("conf.confFilePath");

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream fsin = fs.open(new Path(confFilePath));
		BufferedReader br = new BufferedReader(new InputStreamReader(fsin));

		String line = "";
		while ((line = br.readLine()) != null) {
			String[] temp = line.split(confFileDecollator);
			HashMap<String, String> tmpMap = new HashMap<String, String>();

			// map中是否含有当前数据的kv
			if (map.containsKey(temp[0])) {
				// map包含当前kv
				tmpMap.put("tableName", temp[1]);
				tmpMap.put("field", temp[2]);
				tmpMap.put("spec_id", temp[3]);
				map.get(temp[0]).add(tmpMap);
			} else {
				// map中不包含当前数据kv
				ArrayList<HashMap<String, String>> list = new ArrayList<HashMap<String, String>>();
				tmpMap.put("tableName", temp[1]);
				tmpMap.put("field", temp[2]);
				tmpMap.put("spec_id", temp[3]);
				list.add(tmpMap);
				map.put(temp[0], list);
			}
		}

		this.tableName = context.getConfiguration().get("conf.tableName");
		this.startTime = context.getConfiguration().get("conf.startTime").equals("null") ? Long.MIN_VALUE : praseDate(context.getConfiguration().get("conf.startTime"));
		this.endTime = context.getConfiguration().get("conf.endTime").equals("null") ? Long.MAX_VALUE : praseDate(context.getConfiguration().get("conf.endTime"));
		this.kvName = context.getConfiguration().get("conf.kvName");
		this.endFiledecollator = new String2Hex("0x05").toString();
		this.mos = new MultipleOutputs<NullWritable, Text>(context);
	}

	@Override
	protected void map(ImmutableBytesWritable key, Result result, Mapper<ImmutableBytesWritable, Result, NullWritable, Text>.Context context) throws IOException, InterruptedException {

		rowKey = Bytes.toString(key.get());
		String qualiferFamily = "";

		// 加载hbase中的数据
		HashMap<String, HashMap<String, String>> hbaseMap = new HashMap<String, HashMap<String, String>>();
		
		for (Cell cell : result.rawCells()) {
			HashMap<String, String> map = new HashMap<String, String>();
			
			qualiferFamily = new String(CellUtil.cloneFamily(cell), "UTF-8");
			String qualifer = new String(CellUtil.cloneQualifier(cell), "UTF-8");
			String value = clearValue(new String(CellUtil.cloneValue(cell), "UTF-8"));
			long timeStamp = cell.getTimestamp();

			if (hbaseMap.containsKey(qualifer)) {
				if (Long.parseLong(hbaseMap.get(qualifer).get("timestamp")) < timeStamp) {
					if (timeStamp >= startTime && timeStamp <= endTime) {
						map.put("value", value);
						map.put("timestamp", timeStamp + "");
						map.put("isMatch", "false");
						hbaseMap.put(qualifer, map);
					}
				}
			} else {
				if (timeStamp >= startTime && timeStamp <= endTime) {
					map.put("value", value);
					map.put("timestamp", timeStamp + "");
					map.put("isMatch", "false");
					hbaseMap.put(qualifer, map);
				}
			}
		}

		String ID = hbaseMap.get("ID") == null ? "null" : hbaseMap.get("ID").get("value");
		String SPEC_ID = hbaseMap.get("SPEC_ID") == null ? "null" : hbaseMap.get("SPEC_ID").get("value");

		switch (tableName) {
		case "all":
			allTable(hbaseMap, qualiferFamily, ID, SPEC_ID);
			break;
		case "error":
			errorTable(hbaseMap, qualiferFamily, ID, SPEC_ID);
			break;
		default:
			matchTableName(hbaseMap);
			break;
		}
	}

	@Override
	protected void cleanup(Mapper<ImmutableBytesWritable, Result, NullWritable, Text>.Context context) throws IOException, InterruptedException {
		this.mos.close();
	}

	private long praseDate(String str) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		try {
			Date date = sdf.parse(str);

			return date.getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return 0L;
	}

	// 跑全部字表，包括error
	private void allTable(HashMap<String, HashMap<String, String>> hbaseMap, String qualiferFamily, String ID, String SPEC_ID) {
		// 循环遍历配置文件中的表
		try {
			for (HashMap<String, String> confMap : map.get(kvName)) {
				// 输入的kvName是否等于tableName，如果是，不判断spec_id
				String[] fields = confMap.get("field").split(",");
				String result = "";
				if (kvName.equals(confMap.get("tableName"))) {
					// 判断当前rowkey下每个列，是否在配置表中的字段中
					result = match(hbaseMap, fields);
				} else {
					// 先看spec_id是否在当前配置表数据中存在
					result = spec_Mathc(hbaseMap, fields, confMap.get("spec_id"));
				}
				if (!result.equals("")) {
					this.mos.write(NullWritable.get(), new Text(result), confMap.get("tableName").toLowerCase() + separator + "oss-");
				}
			}

			for (Map.Entry<String, HashMap<String, String>> entry : hbaseMap.entrySet()) {
				if (entry.getValue().get("isMatch").equals("false")) {
					String qualifer = entry.getKey();
					String field1 = "";
					String field2 = "";
					String[] tmps = qualifer.split(".");
					if (tmps.length > 1) {
						field1 = tmps[tmps.length - 1];
						if (!"p".equals(tmps[tmps.length - 2])) {
							field2 = tmps[tmps.length - 2];
						}
					} else {
						field1 = qualifer;
					}

					String errorResponse = rowKey + endFiledecollator + qualiferFamily + endFiledecollator + ID + endFiledecollator + SPEC_ID + endFiledecollator + field1 + endFiledecollator + field2
							+ endFiledecollator + qualifer + endFiledecollator + entry.getValue().get("value") + endFiledecollator + entry.getValue().get("timestamp");

					mos.write(NullWritable.get(), new Text(errorResponse), "error" + separator + kvName.toLowerCase() + separator + "oss-");

				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	// 跑全部error的字表
	private void errorTable(HashMap<String, HashMap<String, String>> hbaseMap, String qualiferFamily, String ID, String SPEC_ID) {
		try {
			for (HashMap<String, String> confMap : map.get(kvName)) {
				// 输入的kvName是否等于tableName，如果是，不判断spec_id
				String[] fields = confMap.get("field").split(",");
				if (kvName.equals(confMap.get("tableName"))) {
					// 判断当前rowkey下每个列，是否在配置表中的字段中
					match(hbaseMap, fields);
				} else {
					// 先看spec_id是否在当前配置表数据中存在
					spec_Mathc(hbaseMap, fields, confMap.get("spec_id"));
				}
			}

			for (Map.Entry<String, HashMap<String, String>> entry : hbaseMap.entrySet()) {
				if (entry.getValue().get("isMatch").equals("false")) {
					String qualifer = entry.getKey();
					String field1 = "";
					String field2 = "";
					String[] tmps = qualifer.split(".");
					if (tmps.length > 1) {
						field1 = tmps[tmps.length - 1];
						if (!"p".equals(tmps[tmps.length - 2])) {
							field2 = tmps[tmps.length - 2];
						}
					} else {
						field1 = qualifer;
					}

					String errorResponse = rowKey + endFiledecollator + qualiferFamily + endFiledecollator + ID + endFiledecollator + SPEC_ID + endFiledecollator + field1 + endFiledecollator + field2
							+ endFiledecollator + qualifer + endFiledecollator + entry.getValue().get("value") + endFiledecollator + entry.getValue().get("timestamp");

					mos.write(NullWritable.get(), new Text(errorResponse), "error" + separator + kvName.toLowerCase() + separator + "oss-");

				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// 匹配指定的字表
	private String matchTableName(HashMap<String, HashMap<String, String>> hbaseMap) {
		try {
			for (HashMap<String, String> confMap : map.get(kvName)) {
				// 输入的kvName是否等于tableName，如果是，不判断spec_id
				String[] fields = confMap.get("field").split(",");
				String result = "";
				if (tableName.equals(confMap.get("tableName"))) {
					if (kvName.equals(confMap.get("tableName"))) {
						// 判断当前rowkey下每个列，是否在配置表中的字段中
						result = match(hbaseMap, fields);
					} else {
						// 先看spec_id是否在当前配置表数据中存在
						result = spec_Mathc(hbaseMap, fields, confMap.get("spec_id"));
					}
					if (!result.equals("")) {
						this.mos.write(NullWritable.get(), new Text(result), "oss-");
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "";
	}

	// 匹配主表
	private String match(HashMap<String, HashMap<String, String>> hbaseMap, String[] fields) throws IOException, InterruptedException {
		StringBuffer sb = new StringBuffer();
		sb.append(rowKey).append(endFiledecollator);
		for (String field : fields) {
			if (hbaseMap.containsKey(field)) {
				hbaseMap.get(field).put("isMatch", "true");
				sb.append(hbaseMap.get(field).get("value")).append(endFiledecollator);
			} else {
				sb.append("null").append(endFiledecollator);
			}
		}
		sb.delete(sb.length() - 1, sb.length());
		return sb.toString();
	}

	// 匹配字表
	private String spec_Mathc(HashMap<String, HashMap<String, String>> hbaseMap, String[] fields, String spec_id) throws IOException, InterruptedException {
		for (String sped_id : spec_id.split(",")) {
			if (sped_id.equals(hbaseMap.get("SPEC_ID").get("value"))) {
				// 再看当前rowkey下每个列是否在配置表中的字段中
				StringBuffer sb = new StringBuffer();
				sb.append(rowKey).append(endFiledecollator);
				for (String field : fields) {
					if (hbaseMap.containsKey(field)) {
						hbaseMap.get(field).put("isMatch", "true");
						sb.append(hbaseMap.get(field).get("value")).append(endFiledecollator);
					} else {
						sb.append("null").append(endFiledecollator);
					}
				}
				sb.delete(sb.length() - 1, sb.length());
				return sb.toString();
			}
		}
		return "";
	}

	private String clearValue(String value) {
		value = value.replaceAll("\r\n|\r|\n|\n\r", "");
		return value;
	}
}
