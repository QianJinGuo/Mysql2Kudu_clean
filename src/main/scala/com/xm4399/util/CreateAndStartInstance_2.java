package com.xm4399.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import org.apache.http.Consts;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.*;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Auther: czk
 * @Date: 2020/7/30
 * @Description:
 */
public class CreateAndStartInstance_2 {
    // 登录并返回token
    public static  String login(){
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost("http://10.0.0.211:8089/api/v1/user/login");
        HttpClientBuilder httpConfig = HttpClientBuilder.create().setDefaultRequestConfig(RequestConfig.custom()
                .setConnectionRequestTimeout(30000)
                .setSocketTimeout(30000)
                .setConnectTimeout(30000).build()).setRetryHandler(new DefaultHttpRequestRetryHandler(3, true));
        HashMap<String, String> headers = new HashMap<String,String>();
        //接收application/json等类型的数据
        headers.put("Accept","application/json, text/plain, */*");
        headers.put("Accept-Encoding","gzip, deflate");
        headers.put("Accept-Language","zh-CN,zh;q=0.9");
        headers.put("Connection","keep-alive");
        //application/json 说明需要穿json数据，所以设置参数的时候要用json格式的数据
        headers.put("Content-Type","application/json;charset=UTF-8");
        headers.put("User-Agent","Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.26 Safari/537.36 Core/1.63.5024.400 QQBrowser/10.0.932.400");
        for (Map.Entry<String, String> entry : headers.entrySet()) {
            httpPost.setHeader(entry.getKey(), entry.getValue());
        }
        JSONObject jsonParam = new JSONObject();
        jsonParam.put("password", "123456");
        jsonParam.put("username", "admin");
        StringEntity entity = new StringEntity(jsonParam.toString(), Consts.UTF_8);
        httpPost.setEntity(entity);
        httpClient = httpConfig.build();
        try {
            CloseableHttpResponse response = httpClient.execute(httpPost);
            int code = response.getStatusLine().getStatusCode();
            if(HttpStatus.SC_OK == code){
                System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>OOOOOOOOOOOOOOOOOOOOOOOOOOO");
                String str = EntityUtils.toString(response.getEntity(), Consts.UTF_8);
                JSONObject jsonObject = JSONObject.parseObject(str);
                String token = jsonObject.getJSONObject("data").getString("token");
                System.out.println(str);
                System.out.println(token);
                System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>OOOOOOOOOOOOOOOOOOOOOOOOO");
                return token;
            }else{
                System.out.println("出错了!! "+ EntityUtils.toString(response.getEntity(), Consts.UTF_8));
            }
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    //创建 instance
    public static String createInstance( String token, String instanceName, String instanceConf) {
        String url = "http://10.0.0.211:8089/api/v1/canal/instance";
        String returnValue = "这是默认返回值，接口调用失败";
        JSONObject jsonParam = new JSONObject();
        jsonParam.put("clusterServerId", "server:1");
        jsonParam.put("content", instanceConf);
        jsonParam.put("name", instanceName);
        CloseableHttpClient httpClient = HttpClients.createDefault();
        ResponseHandler<String> responseHandler = new BasicResponseHandler();
        try{
            httpClient = HttpClients.createDefault();
            HttpPost httpPost = new HttpPost(url);
            // 给httpPost设置JSON格式的参数
            StringEntity requestEntity = new StringEntity(jsonParam.toString(),"utf-8");
            requestEntity.setContentEncoding("UTF-8");
            httpPost.setHeader("Content-type", "application/json;charset=UTF-8");
            httpPost.addHeader("X-Token",token);
            httpPost.setEntity(requestEntity);
            // 发送HttpPost请求，获取返回值
            returnValue = httpClient.execute(httpPost,responseHandler); //调接口获取返回值时，必须用此方法
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        //第五步：处理返回值
        return returnValue;
    }

    // 获取instance的id
    public static String getIstanceID(String token, String instanceName){
        String returnValue = "默认值,没有调用";
        CloseableHttpClient httpClient = HttpClients.createDefault();
        ResponseHandler<String> responseHandler = new BasicResponseHandler();
        try{
            httpClient = HttpClients.createDefault();
            HttpGet httpGet = new HttpGet("http://10.0.0.211:8089/api/v1/canal/instances?name=" + instanceName +"&clusterServerId=&page=&size=");
            httpGet.addHeader("X-Token",token);
            returnValue = httpClient.execute(httpGet,responseHandler); //调接口获取返回值时，必须用此方法
            //System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>以下");
            //System.out.println(returnValue);
            JSONObject jsonObject = JSONObject.parseObject(returnValue);
            JSONObject data = jsonObject.getJSONObject("data");
            JSONArray itemsArr = data.getJSONArray("items");
            JSONObject index0 = itemsArr.getJSONObject(0);
            String id = index0.getString("id");
            System.out.println(id);;
            //System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>以下");
            return id;
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return null;
    }

    // 启动instance
    public static String startInstance(String token, String instanceID) {
        String url = "http://10.0.0.211:8089/api/v1/canal/instance/status/" + instanceID + "?option=start";
        String returnValue = "这是默认返回值，接口调用失败";
        String optionJson = "option:start";
        CloseableHttpClient httpClient = HttpClients.createDefault();
        ResponseHandler<String> responseHandler = new BasicResponseHandler();
        try{
            //第一步：创建HttpClient对象
            httpClient = HttpClients.createDefault();
            //第二步：创建httpPost对象
            HttpPut httpPut = new HttpPut(url);
            //第三步：给httpPut设置JSON格式的参数
            StringEntity requestEntity = new StringEntity(optionJson,"utf-8");
            requestEntity.setContentEncoding("UTF-8");
            httpPut.setHeader("Content-type", "application/json;charset=UTF-8");
            httpPut.addHeader("X-Token",token);
            httpPut.setEntity(requestEntity);
            //第四步：发送HttpPost请求，获取返回值
            returnValue = httpClient.execute(httpPut,responseHandler); //调接口获取返回值时，必须用此方法
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }

        finally {
            try {
                httpClient.close();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        //第五步：处理返回值
        return returnValue;
    }




    public static void createAndStartInstance (String address, String userName, String password, String dbName,
                                               String tableName, String topic) {
        System.out.println(topic);
        String token = login();
        String instanceName = tableName;
        ReaderUtil readerUtil = new ReaderUtil();
        String insanceConf = readerUtil.getInstanceConfString(address, userName, password, dbName, tableName, topic );
        System.out.println("输出instanceConf>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>" +insanceConf);
        createInstance(token, instanceName, insanceConf);
        String instanceID = getIstanceID(token,instanceName);
        startInstance(token,instanceID);
    }

    public static void main(String[] args) {
        createAndStartInstance("ss","ss","ss","ss","dd","ffff");
    }
}
