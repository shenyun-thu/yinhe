
import java.util.Properties;

public class Config {
    public static String es_nodes = "";
    public static String es_port = "";
    public static String es_cluster = "";
    public static String restapi = "";
    public static String log_path = "";
    public static String delay_time = "";
   // public static String slice_num = "";
    public static String now_time = "";
    public static void init(Properties properties){
        es_nodes = properties.getProperty("es_nodes");
        es_port = properties.getProperty("es_port");
        es_cluster = properties.getProperty("es_cluster");
        restapi = properties.getProperty("restapi");
        log_path = properties.getProperty("log_path");
        delay_time = properties.getProperty("delay_time");
       // slice_num = properties.getProperty("slice_num");
        now_time = properties.getProperty("now_time");
    }
}
