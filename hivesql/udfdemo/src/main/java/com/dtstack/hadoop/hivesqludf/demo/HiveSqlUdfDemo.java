package com.dtstack.hadoop.hivesqludf.demo;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author xiaoyu
 * @Create 2021/9/22 10:58
 * @Description
 */
public class HiveSqlUdfDemo extends UDF {
    private static final Logger LOG = LoggerFactory.getLogger(HiveSqlUdfDemo.class);

    /**
     *
     * @param line 输入的json串
     * @param s 需要获取到json串中哪个属性的值
     * @return
     */
    public String evaluate(String line,String s){

        try {
            //将传入的字符串转换成JSONObject对象
            JSONObject jsonObject = new JSONObject(line);
            if("username".equals(s)){
                //通过getString方法可以获取到对应属性名的属性值
                return jsonObject.getString("username");
            }else if("age".equals(s)){
                return jsonObject.getString("age");
            }else {
                /**
                 * 通过getJSONObject方法可以获取属性值为对象的值
                 * 例如{\"username\":\"zs\",\"age\":\"18\",\"computer\":{\"brand\":\"hp\",\"price\":\"7000\"}}
                 * 获取到的就是computer这个对象 然后要想获取对象里对应属性的值再次调用getString方法即可。
                 */
                JSONObject computer = jsonObject.getJSONObject("computer");
                if (computer.has(s)){
                    return computer.getString(s);
                }
            }
        } catch (JSONException e) {
            LOG.error(e.toString());
        }

        return null;
    }

//    public static void main(String[] args) {
//        String s = "{\"username\":\"zs\",\"age\":\"18\",\"computer\":{\"brand\":\"hp\",\"price\":\"7000\"}}";
//        System.out.println(new HiveSqlUdfDemo().evaluate(s,"username"));
//        System.out.println(new HiveSqlUdfDemo().evaluate(s,"age"));
//        System.out.println(new HiveSqlUdfDemo().evaluate(s,"computer"));
//        System.out.println(new HiveSqlUdfDemo().evaluate(s,"brand"));
//        System.out.println(new HiveSqlUdfDemo().evaluate(s,"price"));
//    }
}
