import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.FileInputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class getData {
    public static void main(String args[]){
        if(args.length < 1){
            System.out.println("usage: java -jar aggregation.jar config_path");
            System.exit(0);
        }
        String conf_path = args[0];
        Properties properties = new Properties();
        try{
            properties.load(new FileInputStream(conf_path));
        }catch (Exception e){
            e.printStackTrace();
        }
        Config.init(properties);

        //Settings settings = Settings.builder().put("cluster.name", "apm_es_5").build();
        //Settings settings = Settings.builder().put("cluster.name", "elasticsearch").build();

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'_'HH"); //拼接es读取数据索引需要的时间格式
        SimpleDateFormat dateFormatLog = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        JSONArray[] cond_array = new JSONArray[4];
        cond_array[0] = new JSONArray();
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"0\",\"_trans_ref.bsflag\":\"B\",\"_trans_ref.creditflag\":\"6\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"0\",\"_trans_ref.bsflag\":\"B\",\"_trans_ref.creditflag\":\"a\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"0\",\"_trans_ref.bsflag\":\"0B\",\"_trans_ref.creditflag\":\"6\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"0\",\"_trans_ref.bsflag\":\"0B\",\"_trans_ref.creditflag\":\"a\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"0\",\"_trans_ref.bsflag\":\"S\",\"_trans_ref.creditflag\":\"7\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"0\",\"_trans_ref.bsflag\":\"S\",\"_trans_ref.creditflag\":\"b\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"0\",\"_trans_ref.bsflag\":\"0S\",\"_trans_ref.creditflag\":\"7\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"0\",\"_trans_ref.bsflag\":\"0S\",\"_trans_ref.creditflag\":\"b\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"0\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"1\",\"_trans_ref.bsflag\":\"B\",\"_trans_ref.creditflag\":\"6\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"1\",\"_trans_ref.bsflag\":\"B\",\"_trans_ref.creditflag\":\"a\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"1\",\"_trans_ref.bsflag\":\"0B\",\"_trans_ref.creditflag\":\"6\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"1\",\"_trans_ref.bsflag\":\"0B\",\"_trans_ref.creditflag\":\"a\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"1\",\"_trans_ref.bsflag\":\"S\",\"_trans_ref.creditflag\":\"7\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"1\",\"_trans_ref.bsflag\":\"S\",\"_trans_ref.creditflag\":\"b\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"1\",\"_trans_ref.bsflag\":\"0S\",\"_trans_ref.creditflag\":\"7\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"1\",\"_trans_ref.bsflag\":\"0S\",\"_trans_ref.creditflag\":\"b\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"1\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410502\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[0].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410503\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1] = new JSONArray();
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410301\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410510\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410410\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410512\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410501\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"411015\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410204\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"471031\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"471111\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410413\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410601\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410505\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410610\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"471002\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"471003\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"471012\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_latency_msec\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"411549\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"0\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410502\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[1].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410503\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2] = new JSONArray();
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410301\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410510\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410410\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410512\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410501\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"411015\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410204\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"471031\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"471111\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410413\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410601\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410505\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410610\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"471002\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"471003\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"471012\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_success\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"411549\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410411\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[2].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410502\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3] = new JSONArray();
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410503\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410301\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410510\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410410\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410512\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410501\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"411015\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410204\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"471031\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"471111\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410413\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410601\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410505\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"410610\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"471002\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"471003\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"471012\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));
        cond_array[3].add(JSON.parseObject("{\"Field\":\"_response\",\"_src_ip\":\"218.1.106.42,218.1.106.43\",\"_dst_ip\":\"218.1.106.22,218.1.106.23,218.1.106.24\",\"_sport\":\"21000\",\"_trans_ref.funcid\":\"411549\",\"_trans_ref.market\":\"\",\"_trans_ref.bsflag\":\"\",\"_trans_ref.creditflag\":\"\",\"_ret_code.rc\":\"\"}"));

        InetSocketTransportAddress addr = null;
        try {
            addr = new InetSocketTransportAddress(InetAddress.getByName(Config.es_nodes), Integer.parseInt(Config.es_port));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        long now;
        if(Config.now_time.equals("now")){
            now = System.currentTimeMillis() / 1000;
        }else{
            now = Long.parseLong(Config.now_time);
        }

        while(true){
            long start_time = System.currentTimeMillis();

            long end = now / 10 * 10 - Integer.parseInt(Config.delay_time);
            long start = end - 10;
            System.out.println("now we deal with data from " + dateFormatLog.format(start * 1000) + " to " + dateFormatLog.format(end * 1000));
            String index_of_es = "ezsonar_" + dateFormat.format(start * 1000);

            TransportClient client = null;
            try{
                Settings settings = Settings.builder().put("cluster.name",Config.es_cluster).build();
                client  = new PreBuiltTransportClient(settings).addTransportAddress(addr);
                SearchRequestBuilder search = client.prepareSearch(index_of_es).setTypes("message")
                        .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)  //输出结果无需排序
                        .setScroll(new TimeValue(60000))
                        .setPostFilter(QueryBuilders.boolQuery()      //setPostFilter的方式相当于使用 过滤器 对比与  setQuery的区别就是 搜索得到的文档不打分  但是addSort里似乎已经加了限制  待议
                                .must(QueryBuilders.rangeQuery("_start_at").from(start).to(end).includeLower(true).includeUpper(false)) //10s时间内
                                .must(QueryBuilders.termQuery("_sport",21000)) //端口全部为21000的限制
                                .must(QueryBuilders.rangeQuery("_src_ip").from("218.1.106.42").to("218.1.106.43").includeLower(true).includeUpper(true)) //源IP的限制
                                .must(QueryBuilders.rangeQuery("_dst_ip").from("218.1.106.22").to("218.1.106.24").includeLower(true).includeUpper(true))) //目标IP的限制
                        .setSize(10000)  //一次查询展示 最多为1w条 需要通过下面的scroll方式全量取出
                        .setTimeout(new TimeValue(60000));
                SearchResponse scrollResp= search.get();

                System.out.println("during this time the number of documents found to meet the IP and sport limits is " + scrollResp.getHits().totalHits);
                JSONArray docList = new JSONArray();
                do {
                    for (SearchHit hits : scrollResp.getHits().getHits()) {
                        docList.add(new JSONObject(hits.getSource()));
                    }
                    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).get();
                } while (scrollResp.getHits().getHits().length != 0);

              //  System.out.println("during this time the number of data we get is " + docList.size());
                long read_end = System.currentTimeMillis();
                System.out.println("the time we use to read data from es is " + (read_end - start_time) + " ms");

                final CountDownLatch countDownLatch = new CountDownLatch(4);
                for(int i = 0; i < 4; i++){
                    final int threadID = i;
                    final long timestamp = now;
                    new Thread(() -> {
                        new Calculate(cond_array[threadID],docList,timestamp).cal();
                        countDownLatch.countDown();
                    }).start();
                }
                try{
                    countDownLatch.await();
                }catch (Exception e){
                    e.printStackTrace();
                }
                docList.clear();
                long end_time = System.currentTimeMillis();
                System.out.println("the time we use to calculate is " + (end_time - read_end) + " ms");
                long h = (now % 86400) / 3600 + 8;
                if(h > 8 && h < 15){
                    System.out.println("now is the business time, we push data into the system");
                }else{
                    System.out.println("now is non-business time, we record this data but will not push data");
                }
            } catch(Exception e){
                System.out.println("can not connect to es-cluster / the index does not exist  ");
            }finally {
                if(client!=null){
                    client.close();
                }
                long tmp = System.currentTimeMillis();
                now = now + 10;
                if(now * 1000 > tmp){
                    try{
                        System.out.println("waiting for next batch...");
                        Thread.sleep((now*1000 - tmp));
                    }catch (Exception exception){
                        exception.printStackTrace();
                    }
                }
                System.out.println("-----------------------------------------");
            }
        }
    }
}
