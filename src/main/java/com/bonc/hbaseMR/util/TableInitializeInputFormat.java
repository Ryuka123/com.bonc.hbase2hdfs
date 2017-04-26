package com.bonc.hbaseMR.util;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 * Created by xiabaike on 2016/12/12.
 */
public class TableInitializeInputFormat extends TableInputFormat {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TableInitializeInputFormat.class);

    protected void initialize(JobContext context) throws IOException {
        // Do we have to worry about mis-matches between the Configuration from setConf and the one
        // in this context?
        TableName tableName = TableName.valueOf(getConf().get(INPUT_TABLE));
        try {
            String username = getConf().get("hbase2hdfs.user.name", "hjpt");
            User user = User.create(UserGroupInformation.createRemoteUser(username));
            initializeTable(ConnectionFactory.createConnection(getConf(), user), tableName);
        } catch (Exception e) {
            LOG.error(StringUtils.stringifyException(e));
        }
    }

}
