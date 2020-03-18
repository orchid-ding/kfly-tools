package top.kfly.spark.listener;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.spark.executor.*;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.TaskInfo;
import redis.clients.jedis.Jedis;
import top.kfly.common.JedisUtil;

import java.util.HashMap;

/**
 * spark session 监控
 * @author dingchuangshi
 */
public class SparkSessionListener extends SparkListener {

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        Jedis jedis = JedisUtil.getJedis();
        HashMap<String, String> jedisMap = new HashMap<>();

        TaskMetrics metrics = taskEnd.taskMetrics();

        jedisMap.put("executorCpuTime", metrics.executorCpuTime() + "");
        jedisMap.put("jvmGCTime", metrics.jvmGCTime() + "");
        jedisMap.put("executorDeserializeCpuTime", metrics.executorDeserializeCpuTime() + "");
        jedisMap.put("diskBytesSpilled", metrics.diskBytesSpilled() + "");
        jedisMap.put("executorDeserializeTime", metrics.executorDeserializeTime() + "");
        jedisMap.put("resultSize", metrics.resultSize() + "");

        jedisMap.put("executorCpuTime", metrics.memoryBytesSpilled() + "");
        jedisMap.put("executorCpuTime", metrics.resultSerializationTime() + "");
        jedis.set("taskMetrics", JSON.toJSONString(jedisMap, SerializerFeature.PrettyFormat));


        /**
         * ####################shuffle#######
         */
        ShuffleReadMetrics shuffleReadMetrics = metrics.shuffleReadMetrics();
        ShuffleWriteMetrics shuffleWriteMetrics = metrics.shuffleWriteMetrics();

        HashMap<String, String> shuffleMap = new HashMap<>();
        shuffleMap.put("remoteBlocksFetched", shuffleReadMetrics.remoteBlocksFetched() + "");//shuffle远程拉取数据块
        shuffleMap.put("localBlocksFetched", shuffleReadMetrics.localBlocksFetched() + "");

        shuffleMap.put("writeTime", shuffleWriteMetrics.writeTime() + "");
        shuffleMap.put("remoteBytesRead", shuffleReadMetrics.remoteBytesRead() + ""); //shuffle远程读取的字节数
        shuffleMap.put("localBytesRead", shuffleReadMetrics.localBytesRead() + "");
        shuffleMap.put("fetchWaitTime", shuffleReadMetrics.fetchWaitTime() + "");
        shuffleMap.put("recordsRead", shuffleReadMetrics.recordsRead() + ""); //shuffle读取的记录总数
        shuffleMap.put("bytesWritten", shuffleWriteMetrics.bytesWritten() + ""); //shuffle写的总大小
        shuffleMap.put("recordsWritte", shuffleWriteMetrics.recordsWritten() + ""); //shuffle写的总记录数

        jedis.set("shuffleMetrics",JSON.toJSONString(shuffleMap,SerializerFeature.PrettyFormat));

        /**
         * ####################input   output#######
         */
        InputMetrics inputMetrics = metrics.inputMetrics();
        OutputMetrics outputMetrics = metrics.outputMetrics();
        HashMap<String, String> inPutOutPutHashMap = new HashMap<>();
        inPutOutPutHashMap.put("bytesRead" ,  inputMetrics.bytesRead() +""); //读取的大小
        inPutOutPutHashMap.put("recordsRead" , inputMetrics.recordsRead() +"") ; //总记录数
        inPutOutPutHashMap.put("bytesWritten" , outputMetrics.bytesWritten()+"") ;
        inPutOutPutHashMap.put("recordsWritten" , outputMetrics.recordsWritten()+"");

        jedis.set("inputOutputMetrics",JSON.toJSONString(inPutOutPutHashMap,SerializerFeature.PrettyFormat));


        /**
         * ####################taskInfo#######
         */
        TaskInfo taskInfo = taskEnd.taskInfo();

        HashMap<String, String> taskInfoMap = new HashMap<>();
        taskInfoMap.put("taskId" , taskInfo.taskId() + "") ;
        taskInfoMap.put("host" , taskInfo.host() + "") ;
        // 推测执行
        taskInfoMap.put("speculative" , taskInfo.speculative() + "") ;
        taskInfoMap.put("failed" , taskInfo.failed() + "") ;
        taskInfoMap.put("killed" , taskInfo.killed()+"") ;
        taskInfoMap.put("running" , taskInfo.running()+"");

        jedis.set("taskInfo",JSON.toJSONString(taskInfoMap,SerializerFeature.PrettyFormat));

        JedisUtil.returnJedis(jedis);


    }
}
