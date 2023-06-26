import cn.hutool.core.util.StrUtil;
import org.junit.Test;

import java.util.Arrays;

public class test1 {

    @Test
    public void getPartition(){
        String a = "location:hdfs://nameservice/ns5/vmax_ns5/zxvmax/telecom/lte/dpi/aggr_location_check_d";
        String b = "location:hdfs://nameservice/ns5/vmax_ns5/zxvmax/telecom/lte/dpi/aggr_location_check_d/p_provincecode=110000/p_date=2023-06-22";
        String replaceStr = b.replace(a, "");
        String[] splits = replaceStr.split("/");
        Arrays.asList(splits).stream().forEach(x -> System.out.println(x));
        System.out.println(replaceStr);
    }


    @Test
    public void testSqlFormat(){
        String sqlTemplate = "insert into 'dtsw_data_meta_table_data_info' values (1001, '核心测试', 'hive', '{}', '{}', '{}', '{}', 1, '{}', '{}', '{}', '{}', '{}', '{}', '{}', 1, '{}', null,null);";

        String formattedSql = StrUtil.format(sqlTemplate, "dbname", "TableName", "PartitionStr", "Location", "PartitionProvincecode", "PartitionDate", "PartitionWeek", "PartitionMonth", "PartitionHour", "FileSize", "FileCount", "dateFormat" );
        System.out.println(formattedSql);
    }

}
