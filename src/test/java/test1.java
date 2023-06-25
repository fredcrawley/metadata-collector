import org.junit.Test;

public class test1 {

    @Test
    public void getPartition(){
        String a = "location:hdfs://nameservice/ns5/vmax_ns5/zxvmax/telecom/lte/dpi/aggr_location_check_d";
        String b = "location:hdfs://nameservice/ns5/vmax_ns5/zxvmax/telecom/lte/dpi/aggr_location_check_d/p_provincecode=110000/p_date=2023-06-22";
        System.out.println(b.replace(a,""));
    }

}
