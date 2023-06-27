package com.dtsw;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.map.MapUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.events.*;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.*;

public class CustomListener extends MetaStoreEventListener {

    public HashMap<String, String> partitionKV;

    public static final String PARTITIONPROVINCECODE = "p_provincecode";

    public static final String PARTITIONPDATE = "p_date";

    public static final String PARTITIONREPORTDATE = "reportdate";

    public static final String PARTITIONHOUR = "p_hour";


    public static final String PARTITIONQUARTER = "p_quarter";


    public static final String PARTITION5MIN = "p_5min";


    public static final String PARTITIONYEAR = "p_year";


    public static final String PARTITIONMONTH = "p_month";


    public static final String PARTITIONWEEK = "p_week";


    public static final String TOTALSIZE = "totalSize";


    public static final String NUMFILES = "numFiles";

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
        super.onDropTable(tableEvent);
    }


    @Override
    public void onAddPartition(AddPartitionEvent tableEvent) throws MetaException {
        ArrayList<String> stringArrayList = CollUtil.newArrayList("table: " + tableEvent.getTable(), "partitions: " + CollUtil.join(tableEvent.getPartitionIterator(), ","));

        logWithHeader(tableEvent.getClass().toString() + "-" + CollUtil.join(stringArrayList, ","));

//        String sqlTemplate = "insert into dtsw_data_meta_table_data_info values (1001, 核心测试, 'hive', {}, {}, {}, {}, 1, {}, {}, {}, {}, {}, {}, {}, 1, {}, null,null );";

//        String sqlTemplate = "insert into 'dtsw_data_meta_table_data_info' values (1001, '核心测试', 'hive', '{}', '{}', '{}', '{}', 1, '{}', '{}', '{}', '{}', '{}', '{}', '{}', 1, '{}', null,null);";


//        String sqlTemplate = "insert into dtsw_data_meta_table_data_info values (1001, '核心测试', 'hive', ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 1, ?, null,null ); ";

        String sqlTemplate = "insert into dtsw_data_meta_table_data_info values (1001, '核心测试', 'hive', ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 1, ?, null,?) ON CONFLICT (project_id,cluster_name,database_type,dbname,table_name,partition_info) DO UPDATE set location=EXCLUDED.location,set p_provincecode=EXCLUDED.p_provincecode,set data_day=EXCLUDED.data_day,set data_week=EXCLUDED.data_week,set data_month=EXCLUDED.data_month,set data_hour=EXCLUDED.data_hour,set file_size=EXCLUDED.file_size,set filt_count=EXCLUDED.filt_count,set is_valid=EXCLUDED.is_valid,set update_time=EXCLUDED.update_time;";

        int partitionCount = 1;

        int batchSize = 100;

        Table table = tableEvent.getTable();
        Iterator<Partition> partitionIterator = tableEvent.getPartitionIterator();

//        String url = "jdbc:postgresql://10.37.49.74:5832/dtsw_data_assets";
        String url = "jdbc:postgresql://172.29.209.147:5832/dtsw_data_assets";
        String username = "postgres";
        String password = "U_tywg_2013";

        try (Connection conn = DriverManager.getConnection(url, username, password)) {

            try {

                // 创建Statement对象
                PreparedStatement preparedStatement = conn.prepareStatement(sqlTemplate);

                while (partitionIterator.hasNext()) {

                    LOGGER.info("partitionCount: {}", partitionCount);
                    partitionCount++;

                    Partition partition = partitionIterator.next();

                    Date date = new Date(System.currentTimeMillis());

//            String formattedSql = StrUtil.format(sqlTemplate, getDbName(table), getTableName(table), getPartitionStr(table, partition), getLocation(table), getPartitionProvincecode(table, partition), getPartitionDate(table, partition), getPartitionWeek(table, partition), getPartitionMonth(table, partition), getPartitionHour(table, partition), getFileSize(partition), getFileCount(partition), dateFormat);

                    String dbName = getDbName(table);
                    String tableName = getTableName(table);
                    String partitionStr = getPartitionStr(table, partition);
                    String location = getLocation(table);
                    String partitionProvincecode = getPartitionProvincecode(table, partition);
                    String partitionDate = getPartitionDate(table, partition);
                    String partitionWeek = getPartitionWeek(table, partition);
                    String partitionMonth = getPartitionMonth(table, partition);
                    String partitionHour = getPartitionHour(table, partition);
                    String fileSize = getFileSize(partition);
                    String fileCount = getFileCount(partition);


                    preparedStatement.setString(1, dbName);
                    preparedStatement.setString(2, tableName);
                    preparedStatement.setString(3, partitionStr);
                    preparedStatement.setString(4, location);

                    preparedStatement.setInt(5, null == partitionProvincecode ? -1 : Integer.parseInt(partitionProvincecode));
                    preparedStatement.setString(6, partitionDate);
                    preparedStatement.setInt(7, null == partitionWeek ? -1 : Integer.parseInt(partitionWeek));
                    preparedStatement.setInt(8, null == partitionMonth ? -1 : Integer.parseInt(partitionMonth));
                    preparedStatement.setInt(9, null == partitionHour ? -1 : Integer.parseInt(partitionHour));
                    preparedStatement.setDouble(10, null == fileSize ? -1.0 : Double.parseDouble(fileCount));
                    preparedStatement.setInt(11, null == fileCount ? -1 : Integer.parseInt(fileCount));
                    preparedStatement.setDate(12, date);
                    preparedStatement.setDate(13, date);

                    preparedStatement.addBatch();

                    if (partitionCount % batchSize == 0) {
                        int batch = partitionCount / batchSize;
                        int[] ints = preparedStatement.executeBatch();
                        LOGGER.info("insert 第 {} 批次，批量大小: {}, 返回结果result: {}", batch, batchSize, ints);
                    }
                }

                int[] ints = preparedStatement.executeBatch();
                int remained = partitionCount % batchSize;
                LOGGER.info("insert 剩余批次， 数量: {}，返回结果result: {}", remained, ints);
            } catch (SQLException e) {
                e.printStackTrace();
                conn.rollback();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }


    }


    private void handleAddPartition(String sql) {

        String sqlTemplate = "insert into dtsw_data_meta_table_data_info values (1001, 核心测试, 'hive', ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 1, ?, null,null );";

        // 建立数据库连接
        try (Connection conn = createCon()) {
            // 创建Statement对象
            PreparedStatement preparedStatement = conn.prepareStatement(sqlTemplate);


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public String getMapValue(String key) {

        if (partitionKV.containsKey(key)) {
            String value = partitionKV.get(key);
            return value;
        } else {
            return null;
        }
    }


    public String getPartitionMonth(Table table, Partition partition) {

        if (null == partitionKV) {
            getPartitions(table, partition);
        }

        return getMapValue(PARTITIONMONTH);

    }


    public String getPartitionWeek(Table table, Partition partition) {

        if (null == partitionKV) {
            getPartitions(table, partition);
        }

        return getMapValue(PARTITIONWEEK);

    }


    public String getPartitionHour(Table table, Partition partition) {

        if (null == partitionKV) {
            getPartitions(table, partition);
        }

        return getMapValue(PARTITIONHOUR);

    }


    public String getPartitionDate(Table table, Partition partition) {

        if (null == partitionKV) {
            getPartitions(table, partition);
        }

        if (partitionKV.containsKey(PARTITIONPDATE) || partitionKV.containsKey(PARTITIONREPORTDATE)) {
            String p_date = getMapValue(PARTITIONPDATE);
            return null == p_date ? getMapValue(PARTITIONREPORTDATE) : p_date;
        }

        return null;

    }

    public String getPartitionProvincecode(Table table, Partition partition) {

        if (null == partitionKV) {
            getPartitions(table, partition);
        }

        return getMapValue(PARTITIONPROVINCECODE);

    }


    public void getPartitions(Table table, Partition partition) {
        List<FieldSchema> partitionFieldSchema = table.getPartitionKeys();
        List<String> partitionValues = partition.getValues();
        assert partitionFieldSchema.size() == partitionValues.size();

        HashMap<String, String> partitions = new HashMap<>();

        StringBuilder partitionString = new StringBuilder();

        for (int i = 0; i < partitionFieldSchema.size(); i++) {
            String partitionKey = partitionFieldSchema.get(i).getName();
            String partitionValue = partitionValues.get(i);
            partitions.put(partitionKey, partitionValue);

            partitionString.append("/" + partitionKey + "=" + partitionValue);
        }

        String partitionStr = getPartitionStr(table, partition);

        assert partitionStr.equals(partitionString);

        partitionKV = partitions;

    }


    public String getFileCount(Partition partition) {

        String numFiles = getMapValue(NUMFILES);
        LOGGER.info("numfiles: {}", numFiles);
        return null == numFiles ? null : numFiles;
    }


    public String getFileSize(Partition partition) {
        String totalSize = getMapValue(TOTALSIZE);
        return null == totalSize ? null : totalSize;
    }

    public String getDbType(Table table) {

        return "hive";
    }

    public String getDbName(Table table) {
        String dbName = table.getDbName();

        return dbName;
    }

    public String getTableName(Table table) {
        String tableName = table.getTableName();

        return tableName;
    }

    public String getLocation(Table table) {
        String tableLocation = table.getSd().getLocation();

        return tableLocation;
    }

    public String getPartitionStr(Table table, Partition partition) {
        String tableLocation = table.getSd().getLocation();
        String partitionLocation = partition.getSd().getLocation();

        String partitionStr = partitionLocation.replace(tableLocation, "");
        return partitionStr;
    }

    @Override
    public void onDropPartition(DropPartitionEvent tableEvent) throws MetaException {
        ArrayList<String> stringArrayList = CollUtil.newArrayList("table: " + tableEvent.getTable(), "partitions: " + CollUtil.join(tableEvent.getPartitionIterator(), ","));

        logWithHeader(tableEvent.getClass().toString() + "-" + CollUtil.join(stringArrayList, ","));

        super.onDropPartition(tableEvent);
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

        logWithHeader(insertEvent.getClass().toString() + "-" + CollUtil.join(insertEvent.getFiles(), ","));

        logWithHeader(insertEvent.getClass().toString() + "-" + MapUtil.join(insertEvent.getPartitionKeyValues(), ",", ":"));
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

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomListener.class);
    private static final ObjectMapper objMapper = new ObjectMapper();

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
}