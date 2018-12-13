//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//
//import java.io.*;
//import java.net.HttpURLConnection;
//import java.net.URL;
//import java.util.HashMap;
//import java.util.Map;
//
//class value{
//    double count;
//    int all;
//    value(double count,int all){
//        this.count = count;
//        this.all = all;
//    }
//}
//
//public class test_calculate {
//    JSONArray cond_array;
//    JSONArray docList;
//    long timestamp;
//    public test_calculate(JSONArray cond_array, JSONArray docList, long timestamp){
//        this.cond_array = cond_array;
//        this.docList = docList;
//        this.timestamp = timestamp;
//    }
//
//    static private String executeRequest(String method, String targetURI, String content) {
//        HttpURLConnection connection = null;
//
//        try {
//            URL url = new URL(targetURI);
//            connection = (HttpURLConnection) url.openConnection();
//            connection.setRequestMethod(method);
//            connection.setRequestProperty("Content-Type", "application/json");
//            connection.setRequestProperty("Accept", "application/json");
//            connection.setRequestProperty("contentType", "utf-8");
//            connection.setRequestProperty("Accept-Charset", "utf-8");
//            connection.setRequestProperty("Content-Length", Integer.toString(content.getBytes("utf-8").length));
//            connection.setRequestProperty("Content-Language", "zh-CN");
//            connection.setUseCaches(false);
//            connection.setDoOutput(true);
//
//            if (!method.equals("GET") && !content.isEmpty()) {
//                PrintWriter wr = new PrintWriter(new OutputStreamWriter(connection.getOutputStream(), "utf-8"));
//                wr.print(content);
//                wr.close();
//            }
//
//            InputStream is = connection.getInputStream();
//            BufferedReader rd = new BufferedReader(new InputStreamReader(is, "utf-8"));
//            StringBuilder response = new StringBuilder();
//            String line;
//            while ((line = rd.readLine()) != null) {
//                response.append(line);
//                response.append('\r');
//            }
//            rd.close();
//            return response.toString();
//
//        } catch (Exception e) {
//            return "{}";
//        } finally {
//            if (connection != null)
//                connection.disconnect();
//        }
//    }
//
//    static public String executePost(String targetURI, String content) { return executeRequest("POST", targetURI, content); }
//
//    public void cal(){
//        String[] key_array = {"_trans_ref.funcid","_trans_ref.market","_trans_ref.bsflag","_trans_ref.creditflag","_ret_code.rc"};
//        Map<String, value> map = new HashMap<>();
//
//        int doc_size = docList.size();
//        int cond_size = cond_array.size();
//
//        for (int doc_i = 0; doc_i < doc_size; doc_i ++){
//            JSONObject mes = docList.getJSONObject(doc_i);
//            JSONObject _ret_code = mes.getJSONObject("_ret_code");
//            JSONObject _trans_ref = mes.getJSONObject("_trans_ref");
//            for(int cond_i = 0; cond_i < cond_size; cond_i ++){
//                boolean flag = true;
//                JSONObject cond = cond_array.getJSONObject(cond_i);
//                for(String key : key_array){
//                     if(key.equals("_trans_ref.funcid")){
//                        if(!cond.getString(key).isEmpty()){
//                            if(_trans_ref != null){
//                                if(!cond.getString(key).equals(_trans_ref.getString("funcid"))) { //funcid在trans_ref中不存在或者值为null的情况
//                                    flag = false;
//                                    break;
//                                }
//                            }else{
//                                flag = false;
//                                break;
//                            }
//                        }
//                    }
//                    else if(key.equals("_trans_ref.market")){
//                        if(!cond.getString(key).isEmpty()){
//                            if(_trans_ref != null){
//                                if(!cond.getString(key).equals(_trans_ref.getString("market"))) { //market在trans_ref中不存在或者值为null的情况
//                                    flag = false;
//                                    break;
//                                }
//                            }else{
//                                flag = false;
//                                break;
//                            }
//                        }
//                    }
//                    else if(key.equals("_trans_ref.bsflag")){
//                        if(!cond.getString(key).isEmpty()){
//                            if(_trans_ref != null){
//                                if(!cond.getString(key).equals(_trans_ref.getString("bsflag"))) { //bsflag在trans_ref中不存在或者值为null的情况
//                                    flag = false;
//                                    break;
//                                }
//                            }else{
//                                flag = false;
//                                break;
//                            }
//                        }
//                    }
//                    else if(key.equals("_trans_ref.creditflag")){
//                        if(!cond.getString(key).isEmpty()){
//                            if(_trans_ref != null){
//                                if(!cond.getString(key).equals(_trans_ref.getString("creditflag"))) { //creditflag等条件在trans_ref中不存在或者值为null的情况
//                                    flag = false;
//                                    break;
//                                }
//                            }else{
//                                flag = false;
//                                break;
//                            }
//                        }
//                    }
//                    else if(key.equals("_ret_code.rc")) {
//                        if (!cond.getString(key).isEmpty()) {
//                            if (_ret_code != null) {
//                                if(!cond.getString(key).equals(_ret_code.getString("rc"))){
//                                    flag = false;
//                                    break;
//                                }
//                            } else {
//                                flag = false;
//                                break;
//                            }
//                        }
//                    }
//                }
//                if(flag){
//                    StringBuilder sb = new StringBuilder();
//                    sb.append("kpi.").append(cond.getString("Field")).append(".");
//                    if (!cond.getString("_sport").isEmpty()) {
//                        sb.append("_sport").append(cond.getString("_sport")).append(".");
//                    }
//                    if (!cond.getString("_trans_ref.funcid").isEmpty()) {
//                        sb.append("_funcid").append(cond.getString("_trans_ref.funcid")).append(".");
//                    }
//                    if (!cond.getString("_trans_ref.market").isEmpty()) {
//                        sb.append("_market").append(cond.getString("_trans_ref.market")).append(".");
//                    }
//                    if (!cond.getString("_trans_ref.bsflag").isEmpty()) {
//                        sb.append("_bsflag").append(cond.getString("_trans_ref.bsflag")).append(".");
//                    }
//                    if (!cond.getString("_trans_ref.creditflag").isEmpty()) {
//                        sb.append("_creditflag").append(cond.getString("_trans_ref.creditflag")).append(".");
//                    }
//                    if (!cond.getString("_ret_code.rc").isEmpty()) {
//                        sb.append("_rc").append(cond.getString("_ret_code.rc")).append(".");
//                    }
//                    String key = sb.toString();
//                    key = key.substring(0, key.length() - 1);//去除key拼接过程中最后的一个 "."
//
//                    if(!map.containsKey(key)){
//                        double c = 0.0;
//                        if(key.startsWith("kpi._latency_msec")){
//                            c =  Double.parseDouble(mes.getString("_latency_msec"));
//                        }
//                        if(mes.get("_ret_code") != null) {
//                            if (key.startsWith("kpi._success")) {
//                                if ("0".equals(_ret_code.getString("rc"))) {
//                                    c = 1.0;
//                                }
//                            }
//                            if (key.startsWith("kpi._response")) {
//                                if ("noresponse".equals(_ret_code.getString("probe_st"))) {
//                                    c = 1.0;
//                                }
//                            }
//                        }
//                        int a = 1;
//                        map.put(key,new value(c,a));
//                    }
//                    else{
//                        double c = map.get(key).count;
//                        if(key.startsWith("kpi._latency_msec")){
//                            c += Double.parseDouble(mes.getString("_latency_msec"));
//                        }
//                        if(mes.get("_ret_code") != null) {
//                            if (key.startsWith("kpi._success")) {
//                                if ("0".equals(_ret_code.getString("rc"))) {
//                                    c += 1.0;
//                                }
//                            }
//                            if (key.startsWith("kpi._response")) {
//                                if ("noresponse".equals(_ret_code.getString("probe_st"))) {
//                                    c += 1.0;
//                                }
//                            }
//                        }
//                        int a = map.get(key).all + 1;
//                        map.put(key,new value(c,a));
//                    }
//                }
//            }
//        }
//        JSONArray jsonArray = new JSONArray();
//        for (String k : map.keySet()){
//            JSONObject jsonObject = new JSONObject();
//            jsonObject.put("kpi",k);
//            jsonObject.put("timestamp",timestamp);
//            if(k.startsWith("kpi._response")){
//                jsonObject.put("value",1 - map.get(k).count/map.get(k).all);
//            }else{
//                jsonObject.put("value",map.get(k).count/map.get(k).all);
//            }
//            jsonObject.put("interval",10);
//            jsonArray.add(jsonObject);
//        }
//        System.out.println(jsonArray.toString());
//        String url = "http://" + Config.restapi + "/api/kpi/kafka";
//        long h = (timestamp % 86400) / 3600 + 8;
//        if(h > 8 && h < 15){
//            executePost(url,jsonArray.toString());
//        }
//    }
//}
