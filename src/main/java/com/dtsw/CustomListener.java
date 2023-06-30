package com.dtsw;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;
import java.util.*;


public class CustomListener extends MetaStoreEventListener {


    private static final Logger LOGGER = LoggerFactory.getLogger(CustomListener.class);

    private static final ObjectMapper objMapper = new ObjectMapper();

    public static Map<String, String> partitionKV = new HashMap<String, String>(10);

    public static final String PARTITION_PROVINCECODE = "p_provincecode";

    public static final String PARTITION_PDATE = "p_date";

    public static final String PARTITION_REPORTDATE = "reportdate";

    public static final String PARTITION_HOUR = "p_hour";


    public static final String PARTITION_QUARTER = "p_quarter";


    public static final String PARTITION_5MIN = "p_5min";


    public static final String PARTITION_YEAR = "p_year";


    public static final String PARTITION_MONTH = "p_month";


    public static final String PARTITION_WEEK = "p_week";


    public static final String TOTAL_SIZE = "totalSize";


    public static final String NUM_FILES = "numFiles";


    public static final String EXTERNAL_TABLE = "EXTERNAL_TABLE";


    public static final String MANAGED_TABLE = "MANAGED_TABLE";


    public static final String VMAX_CONFIG_PATH = "/etc/zdh/spark/conf.zdh.spark/dtsw-table-metadata-collector.properties";


    public static final String QZ_CONFIG_PATH = "/home/hypergalaxy/spark/conf/dtsw-table-metadata-collector.properties";


    public static final int PROJECT_ID = 1001;


    public static final String DATABASE_TYPE = "hive";


    public static final String CLUSTER_NAME_KEY = "cluster_name";


    public static String CLUSTER_NAME;


    public static final String PG_URL = "jdbc:postgresql://10.37.49.74:5832/dtsw_data_assets";

    public static final String PG_USERNAME = "postgres";

    public static final String PG_PASSWORD = "U_tywg_2013";


    @Override
    public void onAddPartition(AddPartitionEvent tableEvent) throws MetaException {
        ArrayList<String> stringArrayList = CollUtil.newArrayList("table: " + tableEvent.getTable(), "partitions: " + CollUtil.join(tableEvent.getPartitionIterator(), ","));

        logWithHeader(tableEvent.getClass().toString() + "-" + CollUtil.join(stringArrayList, ","));


        String sqlTemplate = "insert into dtsw_data_meta_table_data_info2 values (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 1, ?, null,?) ON CONFLICT (project_id,cluster_name,database_type,dbname,table_name,partition_info) DO UPDATE set location=EXCLUDED.location, p_provincecode=EXCLUDED.p_provincecode, data_day=EXCLUDED.data_day, data_week=EXCLUDED.data_week, data_month=EXCLUDED.data_month, data_hour=EXCLUDED.data_hour, file_size=EXCLUDED.file_size, file_count=EXCLUDED.file_count, is_valid=EXCLUDED.is_valid, update_time=EXCLUDED.update_time;";

        int partitionCount = 0;

        int batchSize = 3;

        Table table = tableEvent.getTable();
        Iterator<Partition> partitionIterator = tableEvent.getPartitionIterator();


        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USERNAME, PG_PASSWORD)) {

            try {

                PreparedStatement preparedStatement = conn.prepareStatement(sqlTemplate);

                conn.setAutoCommit(false);

                while (partitionIterator.hasNext()) {

                    partitionCount++;
                    LOGGER.info("partitionCount: {}", partitionCount);

                    Partition partition = partitionIterator.next();
                    Map<String, String> parameters = partition.getParameters();

                    LOGGER.info("partition size and files: {}, {}, keys: {}", getMapValue(parameters, TOTAL_SIZE), getMapValue(parameters, NUM_FILES), null == parameters ? null : parameters.keySet().toString());

                    setPartitionKV(table, partition);

                    LOGGER.info("partitionKV 的size为{}， 值为 {}", partitionKV.size(), MapUtil.join(partitionKV, ",", ":"));

                    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                    String dbName = getDbName(table);
                    String tableName = getTableName(table);
                    String partitionStr = getPartitionStr(table, partition);
                    String location = getLocation(table);
                    String partitionProvincecode = getMapValue(partitionKV, PARTITION_PROVINCECODE);
                    String partitionDate = getPartitionDate();
                    String partitionWeek = getMapValue(partitionKV, PARTITION_WEEK);
                    String partitionMonth = getMapValue(partitionKV, PARTITION_MONTH);
                    String partitionHour = getMapValue(partitionKV, PARTITION_HOUR);
                    String fileSize = getMapValue(parameters, TOTAL_SIZE);
                    String fileCount = getMapValue(parameters, NUM_FILES);

                    preparedStatement.setInt(1, PROJECT_ID);
                    preparedStatement.setString(2, CLUSTER_NAME);
                    preparedStatement.setString(3, DATABASE_TYPE);
                    preparedStatement.setString(4, dbName);
                    preparedStatement.setString(5, tableName);
                    preparedStatement.setString(6, partitionStr);
                    preparedStatement.setString(7, location);

                    setIntOrNull(preparedStatement, 8, partitionProvincecode);
                    preparedStatement.setString(9, partitionDate);

                    setIntOrNull(preparedStatement, 10, partitionWeek);
                    setIntOrNull(preparedStatement, 11, partitionMonth);
                    setIntOrNull(preparedStatement, 12, partitionHour);
                    setIntOrDoubleFileSize(preparedStatement, 13, fileSize);
                    setIntOrNull(preparedStatement, 14, fileCount);

                    preparedStatement.setTimestamp(15, timestamp);
                    preparedStatement.setTimestamp(16, timestamp);

                    preparedStatement.addBatch();

                    if (partitionCount % batchSize == 0) {

                        int batch = partitionCount / batchSize;
                        int[] results = preparedStatement.executeBatch();
                        conn.commit();

                        LOGGER.info("insert 第 {} 批次，批量大小: {}, 返回结果result: {}", batch, batchSize, Arrays.toString(results));
                        for (int result : results) {
                            if (result == PreparedStatement.EXECUTE_FAILED) {
                                conn.rollback();
                                LOGGER.info("insert 第 {} 批次，批量大小: {} 失败，回滚事务", batch, batchSize);
                            }
                        }

                    }

                    partitionKV.clear();
                }

                if (partitionCount % batchSize > 0) {
                    int[] results = preparedStatement.executeBatch();
                    conn.commit();

                    int remained = partitionCount % batchSize;
                    for (int result : results) {
                        if (result == PreparedStatement.EXECUTE_FAILED) {
                            conn.rollback(); // 回滚事务
                            LOGGER.info("insert 剩余批次， 数量: {} 失败，回滚事务", batchSize);
                        }
                    }
                    LOGGER.info("insert 剩余批次， 数量: {}，返回结果 result: {}", remained, Arrays.toString(results));
                }

            } catch (SQLException e) {
                e.printStackTrace();
                conn.rollback();
                LOGGER.info("addPartition 失败回滚");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void onConfigChange(ConfigChangeEvent tableEvent) throws MetaException {
        ArrayList<String> stringArrayList = CollUtil.newArrayList("key: " + tableEvent.getKey(), "new value: " + tableEvent.getNewValue(), "old value: " + tableEvent.getOldValue());

        logWithHeader(tableEvent.getClass().toString() + "-" + CollUtil.join(stringArrayList, ","));

        super.onConfigChange(tableEvent);
    }

    @Override
    public void onDropTable(DropTableEvent tableEvent) throws MetaException {
        ArrayList<String> stringArrayList = CollUtil.newArrayList("table: " + tableEvent.getTable(), "isDeleteData: " + tableEvent.getDeleteData());

        logWithHeader(tableEvent.getClass().toString() + "-" + CollUtil.join(stringArrayList, ","));

        Table table = tableEvent.getTable();

        String tableType = table.getTableType();
        int isValid = isValid(tableType);

        String sqlTemplate = "update dtsw_data_meta_table_data_info2 set is_valid = ?, delete_time = ?, update_time = ? where project_id = ? and cluster_name = ? and database_type = ? and dbname = ? and table_name = ?;";

        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USERNAME, PG_PASSWORD)) {

            try {

                // 创建Statement对象
                PreparedStatement preparedStatement = conn.prepareStatement(sqlTemplate);

                conn.setAutoCommit(false);

                Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                String dbName = getDbName(table);
                String tableName = getTableName(table);

                preparedStatement.setInt(1, isValid);
                preparedStatement.setTimestamp(2, timestamp);
                preparedStatement.setTimestamp(3, timestamp);
                preparedStatement.setInt(4, PROJECT_ID);
                preparedStatement.setString(5, CLUSTER_NAME);
                preparedStatement.setString(6, DATABASE_TYPE);
                preparedStatement.setString(7, dbName);
                preparedStatement.setString(8, tableName);

                preparedStatement.addBatch();

            } catch (SQLException e) {
                e.printStackTrace();
                LOGGER.info("drop table HMS 监听写入pg 失败回滚 ");
                conn.rollback();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }




    @Override
    public void onDropPartition(DropPartitionEvent tableEvent) throws MetaException {
        ArrayList<String> stringArrayList = CollUtil.newArrayList("table: " + tableEvent.getTable(), "partitions: " + CollUtil.join(tableEvent.getPartitionIterator(), ","));

        logWithHeader(tableEvent.getClass().toString() + "-" + CollUtil.join(stringArrayList, ","));


        String sqlTemplate = "update dtsw_data_meta_table_data_info2 set is_valid = ?, delete_time = ?, update_time = ? where project_id = ? and cluster_name = ? and database_type = ? and dbname = ? and table_name = ? and partition_info = ?;";


        int partitionCount = 0;

        int batchSize = 3;

        Table table = tableEvent.getTable();
        Iterator<Partition> partitionIterator = tableEvent.getPartitionIterator();

        String tableType = table.getTableType();
        int isValid = isValid(tableType);


        try (Connection conn = DriverManager.getConnection(PG_URL, PG_USERNAME, PG_PASSWORD)) {

            try {

                // 创建Statement对象
                PreparedStatement preparedStatement = conn.prepareStatement(sqlTemplate);

                conn.setAutoCommit(false);

                while (partitionIterator.hasNext()) {

                    partitionCount++;
                    LOGGER.info("partitionCount: {}", partitionCount);

                    Partition partition = partitionIterator.next();
                    LOGGER.info("partition size and files: {}, {}, keys: {}", partition.getParameters().get(TOTAL_SIZE), partition.getParameters().get(NUM_FILES), partition.getParameters().keySet().toString());

                    Timestamp timestamp = new Timestamp(System.currentTimeMillis());

                    String dbName = getDbName(table);
                    String tableName = getTableName(table);
                    String partitionStr = getPartitionStr(table, partition);

                    preparedStatement.setInt(1, isValid);
                    preparedStatement.setTimestamp(2, timestamp);
                    preparedStatement.setTimestamp(3, timestamp);
                    preparedStatement.setInt(4, PROJECT_ID);
                    preparedStatement.setString(5, CLUSTER_NAME);
                    preparedStatement.setString(6, DATABASE_TYPE);
                    preparedStatement.setString(7, dbName);
                    preparedStatement.setString(8, tableName);
                    preparedStatement.setString(9, partitionStr);

                    preparedStatement.addBatch();

                    if (partitionCount % batchSize == 0) {

                        int batch = partitionCount / batchSize;
                        int[] results = preparedStatement.executeBatch();
                        conn.commit();

                        LOGGER.info("drop partition update 第 {} 批次，批量大小: {}, 返回结果result: {}", batch, batchSize, Arrays.toString(results));
                        for (int result : results) {
                            if (result == PreparedStatement.EXECUTE_FAILED) {
                                conn.rollback(); // 回滚事务
                                LOGGER.info("drop partition  第 {} 批次，批量大小: {} 失败，回滚事务", batch, batchSize);
                            }
                        }

                    }
                }

                if (partitionCount % batchSize > 0) {
                    int[] results = preparedStatement.executeBatch();
                    conn.commit();

                    int remained = partitionCount % batchSize;
                    for (int result : results) {
                        if (result == PreparedStatement.EXECUTE_FAILED) {
                            conn.rollback(); // 回滚事务
                            LOGGER.info("drop partition  剩余批次， 数量: {} 失败，回滚事务", batchSize);
                        }
                    }
                    LOGGER.info("drop partition  剩余批次， 数量: {}，返回结果 result: {}", remained, Arrays.toString(results));
                }

            } catch (SQLException e) {
                e.printStackTrace();
                LOGGER.info("drop partition 失败回滚 ");
                conn.rollback();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onAlterPartition(AlterPartitionEvent tableEvent) throws MetaException {
        ArrayList<String> stringArrayList = CollUtil.newArrayList("table: " + tableEvent.getTable(), "newPartitions: " + tableEvent.getNewPartition(), "oldPartitions:" + tableEvent.getOldPartition());

        logWithHeader(tableEvent.getClass().toString() + "-" + CollUtil.join(stringArrayList, ","));

    }

    @Override
    public void onCreateDatabase(CreateDatabaseEvent dbEvent) throws MetaException {
        super.onCreateDatabase(dbEvent);
    }

    @Override
    public void onDropDatabase(DropDatabaseEvent dbEvent) throws MetaException {
        super.onDropDatabase(dbEvent);
    }

    @Override
    public void onLoadPartitionDone(LoadPartitionDoneEvent partSetDoneEvent) throws MetaException {
        logWithHeader(partSetDoneEvent.getClass().toString() + "-" + MapUtil.join(partSetDoneEvent.getPartitionName(), ",", ":"));
        super.onLoadPartitionDone(partSetDoneEvent);
    }

    @Override
    public void onAddIndex(AddIndexEvent indexEvent) throws MetaException {
        super.onAddIndex(indexEvent);
    }

    @Override
    public void onDropIndex(DropIndexEvent indexEvent) throws MetaException {
        super.onDropIndex(indexEvent);
    }

    @Override
    public void onAlterIndex(AlterIndexEvent indexEvent) throws MetaException {
        super.onAlterIndex(indexEvent);
    }

    @Override
    public void onInsert(InsertEvent insertEvent) throws MetaException {

        ArrayList<String> DbAndtable = CollUtil.newArrayList(insertEvent.getClass().toString() + "-", " db: " + insertEvent.getDb(), "table: " + insertEvent.getTable());
        logWithHeader(CollUtil.join(DbAndtable, ","));

        logWithHeader(insertEvent.getClass() + "-" + CollUtil.join(insertEvent.getFiles(), ","));

        logWithHeader(insertEvent.getClass() + "-" + MapUtil.join(insertEvent.getPartitionKeyValues(), ",", ":"));
        super.onInsert(insertEvent);
    }

    @Override
    public Configuration getConf() {
        return super.getConf();
    }

    @Override
    public void setConf(Configuration config) {
        super.setConf(config);
    }


    public CustomListener(Configuration config) {
        super(config);
        logWithHeader(" created ");
    }

    @Override
    public void onCreateTable(CreateTableEvent event) {
        logWithHeader("CreateTableEvent - new table:" + event.getTable().toString());
    }

    @Override
    public void onAlterTable(AlterTableEvent event) {
        logWithHeader("AlterTableEvent - old table:" + event.getOldTable().toString());
        logWithHeader("AlterTableEvent - new table:" + event.getNewTable().toString());
    }

    private void logWithHeader(String str) {
        LOGGER.info("[CustomListener][Thread: " + Thread.currentThread().getName() + "] | " + objToStr(str));
    }


    public Connection createCon() {
        String url = "jdbc:postgresql://10.37.49.74:5832/dtsw_data_assets";
        String username = "postgres";
        String password = "U_tywg_2013";
        Connection conn = null;

        try (Connection conTemp = DriverManager.getConnection(url, username, password)) {
            conn = conTemp;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    private String objToStr(String str) {
        try {
            return objMapper.writeValueAsString(str);
        } catch (IOException e) {
            LOGGER.error("Error on conversion", e);
        }
        return null;
    }


    public void setIntOrNull(PreparedStatement statement, int parameterIndex, String value) {

        try {
            if (null == value) {
                statement.setNull(parameterIndex, Types.INTEGER);
            } else {
                statement.setInt(parameterIndex, Integer.parseInt(value));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void setIntOrDoubleFileSize(PreparedStatement statement, int parameterIndex, String value) {

        try {
            if (null == value) {
                statement.setNull(parameterIndex, Types.DOUBLE);
            } else {
                statement.setDouble(parameterIndex, Double.parseDouble(value) / 1024 / 1024);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public String getMapValue(Map<String, String> map, String key) {

        if (null == map) {
            return null;
        }

        if (map.containsKey(key)) {
            return map.get(key);
        } else {
            return null;
        }
    }





    public void setPartitionKV(Table table, Partition partition) {
        List<FieldSchema> partitionFieldSchema = table.getPartitionKeys();
        List<String> partitionValues = partition.getValues();
        assert partitionFieldSchema.size() == partitionValues.size();


        StringBuilder partitionString = new StringBuilder();

        for (int i = 0; i < partitionFieldSchema.size(); i++) {
            String partitionKey = partitionFieldSchema.get(i).getName();
            String partitionValue = partitionValues.get(i);
            partitionKV.put(partitionKey, partitionValue);

            partitionString.append("/").append(partitionKey).append("=").append(partitionValue);
        }


    }


    public String getDbType(Table table) {

        return "hive";
    }

    public String getDbName(Table table) {

        return table.getDbName();
    }

    public String getTableName(Table table) {

        return table.getTableName();
    }

    public String getLocation(Table table) {

        return table.getSd().getLocation();
    }

    public String getPartitionStr(Table table, Partition partition) {
        String tableLocation = table.getSd().getLocation();
        String partitionLocation = partition.getSd().getLocation();

        String partitionStr = partitionLocation.replace(tableLocation, "");
        return partitionStr;
    }


    public String getPartitionDate() {

        if (partitionKV.containsKey(PARTITION_PDATE) || partitionKV.containsKey(PARTITION_REPORTDATE)) {
            String p_date = getMapValue(partitionKV, PARTITION_PDATE);
            return null == p_date ? getMapValue(partitionKV, PARTITION_REPORTDATE) : p_date;
        }

        return null;
    }


    public int isValid(String tableType){
        LOGGER.info("tableType is: {}", tableType);
        if (MANAGED_TABLE.equals(tableType)) {
            return 0;
        }
        return 1;
    }



    @Deprecated
    public void setCommonConfig() {

        try {
            Properties properties = new Properties();
            File vmaxConfig = new File(VMAX_CONFIG_PATH);
            File qzConfig = new File(QZ_CONFIG_PATH);

            if (vmaxConfig.exists()) {

                FileInputStream fileInputStream = new FileInputStream(qzConfig);
                properties.load(fileInputStream);

                String cluster_name = properties.getProperty(CLUSTER_NAME_KEY);
                CLUSTER_NAME = cluster_name;

            } else if (vmaxConfig.exists()) {
                FileInputStream fileInputStream
                        = new FileInputStream(qzConfig);
                properties.load(fileInputStream);

                String cluster_name = properties.getProperty(CLUSTER_NAME_KEY);
                CLUSTER_NAME = cluster_name;

            } else {

                throw new Exception("未读取到配置文件，请核查原因！\n config file [" + vmaxConfig + "," + qzConfig + "] not found! ");
            }

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


    static {
        try {

            Properties properties = new Properties();
            File vmaxConfig = new File(VMAX_CONFIG_PATH);
            File qzConfig = new File(QZ_CONFIG_PATH);

            if (vmaxConfig.exists()) {

                FileInputStream fileInputStream = new FileInputStream(vmaxConfig);
                BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream, "UTF-8"));
                properties.load(reader);

                CLUSTER_NAME = properties.getProperty(CLUSTER_NAME_KEY);
                LOGGER.info("load config file：{} successfully!", vmaxConfig);
            } else if (qzConfig.exists()) {

                FileInputStream fileInputStream = new FileInputStream(qzConfig);
                BufferedReader reader = new BufferedReader(new InputStreamReader(fileInputStream, "UTF-8"));
                properties.load(reader);

                CLUSTER_NAME = properties.getProperty(CLUSTER_NAME_KEY);
                LOGGER.info("load config file：{} successfully!", qzConfig);
            } else {

                throw new Exception("未读取到配置文件，请核查原因！\n config file [" + vmaxConfig + "," + qzConfig + "] not found! ");
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}