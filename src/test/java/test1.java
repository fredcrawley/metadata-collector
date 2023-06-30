import cn.hutool.core.util.StrUtil;
import com.dtsw.CustomListener;
import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class test1 {

    private static final Logger LOGGER = LoggerFactory.getLogger(test1.class);

    @Test
    public void getPartition() {
        String a = "location:hdfs://nameservice/ns5/vmax_ns5/zxvmax/telecom/lte/dpi/aggr_location_check_d";
        String b = "location:hdfs://nameservice/ns5/vmax_ns5/zxvmax/telecom/lte/dpi/aggr_location_check_d/p_provincecode=110000/p_date=2023-06-22";
        String replaceStr = b.replace(a, "");
        String[] splits = replaceStr.split("/");
        Arrays.asList(splits).stream().forEach(x -> System.out.println(x));
        System.out.println(replaceStr);
    }


    @Test
    public void testSqlFormat() {
        String sqlTemplate = "insert into 'dtsw_data_meta_table_data_info' values (1001, '核心测试', 'hive', '{}', '{}', '{}', '{}', 1, '{}', '{}', '{}', '{}', '{}', '{}', '{}', 1, '{}', null,null);";

        String formattedSql = StrUtil.format(sqlTemplate, "dbname", "TableName", "PartitionStr", "Location", "PartitionProvincecode", "PartitionDate", "PartitionWeek", "PartitionMonth", "PartitionHour", "FileSize", "FileCount", "dateFormat");
        System.out.println(formattedSql);


    }


    public String getMapValue(String key) {
        HashMap<String, String> partitionKV = new HashMap<>();
        partitionKV.put("a", "b");

        if (partitionKV.containsKey(key)) {
            String value = partitionKV.get(key);
            return value;
        } else {
            return null;
        }
    }

    @Test
    public void testGetMap() {
//        getFileCount();

        Date date = new Date();
        String s = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").toString();
        System.out.println(s);
    }


    public int getFileCount() {

        String numFiles = getMapValue("NUMFILES");

        if (null == numFiles) {
            System.out.println("it is null");
        } else {
            System.out.println("not null");
            System.out.println(Integer.parseInt(numFiles));
        }
//        return  null == numFiles ? null : Integer.parseInt(numFiles);
        return 3;
    }


    @Test
    public void testArray(){

         int[] a = {1,5,2,72,6};
        System.out.println(Arrays.toString(a));

        HashMap<String, String> stringStringHashMap = new HashMap<>();
        stringStringHashMap.put("a", "b");
        stringStringHashMap.put("c", "d");
        System.out.println(stringStringHashMap.keySet().toString());

        File qzConfig = new File("QZ_CONFIG_PATH");
        System.out.println(qzConfig.exists());
    }


    public static final int PROJECT_ID = 1001;


    public static final String DATABASE_TYPE = "hive";


    public static final String CLUSTER_NAME_KEY = "cluster_name";


    public static String CLUSTER_NAME = "核心测试";
    @Test
    public void insertPg() {

//        String sqlTemplate = "insert into dtsw_data_meta_table_data_info2 values (1001, '核心测试', 'hive', ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 1, ?, null,?) ON CONFLICT (project_id,cluster_name,database_type,dbname,table_name,partition_info) DO UPDATE set location=EXCLUDED.location, p_provincecode=EXCLUDED.p_provincecode, data_day=EXCLUDED.data_day, data_week=EXCLUDED.data_week, data_month=EXCLUDED.data_month, data_hour=EXCLUDED.data_hour, file_size=EXCLUDED.file_size, file_count=EXCLUDED.file_count, is_valid=EXCLUDED.is_valid, update_time=EXCLUDED.update_time;";

        String sqlTemplate = "insert into dtsw_data_meta_table_data_info2 values (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, 1, ?, null,?) ON CONFLICT (project_id,cluster_name,database_type,dbname,table_name,partition_info) DO UPDATE set location=EXCLUDED.location, p_provincecode=EXCLUDED.p_provincecode, data_day=EXCLUDED.data_day, data_week=EXCLUDED.data_week, data_month=EXCLUDED.data_month, data_hour=EXCLUDED.data_hour, file_size=EXCLUDED.file_size, file_count=EXCLUDED.file_count, is_valid=EXCLUDED.is_valid, update_time=EXCLUDED.update_time;";

//        String url = "jdbc:postgresql://localhost:5832/dtsw_data_assets";
        String url = "jdbc:postgresql://172.16.1.128:5832/dtsw_data_assets";
        String username = "postgres";
        String password = "U_tywg_2013";

        try (Connection conn = DriverManager.getConnection(url, username, password)) {

            try {

                // 创建Statement对象
                PreparedStatement preparedStatement = conn.prepareStatement(sqlTemplate);

                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                conn.setAutoCommit(false);

                String dbName = "dbname";
                String tableName = "tablename";
                String partitionStr = "partitionStr2";
                String location = "location";
                String partitionProvincecode = "110000";
                String partitionDate = "2023-06-10";
//                String partitionWeek = "1";
                String partitionWeek = null;
                String partitionMonth = "1";
                String partitionHour = "1";
                String fileSize = null;
                String fileCount = "1";


                preparedStatement.setInt(1, PROJECT_ID);
                preparedStatement.setString(2, CLUSTER_NAME);
                preparedStatement.setString(3, DATABASE_TYPE);
                preparedStatement.setString(4, dbName);
                preparedStatement.setString(5, tableName);
                preparedStatement.setString(6, partitionStr);
                preparedStatement.setString(7, location);

//                    preparedStatement.setInt(5, null == partitionProvincecode ? -1 : Integer.parseInt(partitionProvincecode));

                setIntOrNull(preparedStatement, 8, partitionProvincecode);
                preparedStatement.setString(9, partitionDate);
//                    preparedStatement.setInt(7, null == partitionWeek ? -1 : Integer.parseInt(partitionWeek));
//                    preparedStatement.setInt(8, null == partitionMonth ? -1 : Integer.parseInt(partitionMonth));
//                    preparedStatement.setInt(9, null == partitionHour ? -1 : Integer.parseInt(partitionHour));
//                    preparedStatement.setDouble(10, null == fileSize ? -1.0 : Double.parseDouble(fileSize));
//                    preparedStatement.setInt(11, null == fileCount ? -1 : Integer.parseInt(fileCount));

                setIntOrNull(preparedStatement, 10, partitionWeek);
                setIntOrNull(preparedStatement, 11, partitionMonth);
                setIntOrNull(preparedStatement, 12, partitionHour);
                setIntOrDoubleFileSize(preparedStatement, 13, fileSize);
                setIntOrNull(preparedStatement, 14, fileCount);

                preparedStatement.setTimestamp(15, timestamp);
                preparedStatement.setTimestamp(16, timestamp);

                preparedStatement.addBatch();


                int[] results = preparedStatement.executeBatch();
                for (int result : results) {
                    if (result == PreparedStatement.EXECUTE_FAILED) {
                        conn.rollback(); // 回滚事务
                        System.out.println("回滚事务" + Arrays.toString(results));
                    }
                }
                System.out.println("insert 剩余批次， 数量: {}，返回结果result:" + Arrays.toString(results));
                conn.commit();
            } catch (SQLException e) {
                e.printStackTrace();
                conn.rollback();
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
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
                statement.setDouble(parameterIndex, Double.parseDouble(value));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


}
