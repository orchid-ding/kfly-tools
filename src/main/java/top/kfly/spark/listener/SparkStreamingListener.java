package top.kfly.spark.listener;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.scheduler.*;
import redis.clients.jedis.Jedis;
import top.kfly.common.JedisUtil;
import top.kfly.common.TimeUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * spark streaming listener 监控
 * @author dingchuangshi
 */
public class SparkStreamingListener implements StreamingListener {


    private Logger log = Logger.getLogger("sparkStreamingLogger");

    private Map<String,String> mapDatas = new HashMap<>();

    private int finishBatchNum  = 0;

    private int duration;

    public SparkStreamingListener(int duration){
        this.duration = duration;
    }

    @Override
    public void onStreamingStarted(StreamingListenerStreamingStarted streamingStarted) {
        log.info("startingStream" + TimeUtils.formatDate(System.currentTimeMillis(),"yyyy-MM-dd HH:mm:ss"));
    }

    @Override
    public void onBatchStarted(StreamingListenerBatchStarted batchStarted) {
        // 调度延迟时间为
        mapDatas.put("scheduleDelay",batchStarted.batchInfo().schedulingDelay().get().toString());
    }

    @Override
    public void onBatchSubmitted(StreamingListenerBatchSubmitted batchSubmitted) {
        // 数据提交数量
        mapDatas.put("submitRecords",batchSubmitted.batchInfo().numRecords() + "");

    }


    @Override
    public void onBatchCompleted(StreamingListenerBatchCompleted batchCompleted) {
        Jedis jedis = JedisUtil.getJedis();

        // 该批次完成调用
        // 该批次数据处理总时间
        String totalDelay = batchCompleted.batchInfo().totalDelay().get().toString();
        //完成数量
        long records = batchCompleted.batchInfo().numRecords();
        long processingStratTime = (long) batchCompleted.batchInfo().processingStartTime().get();
        long processingEndTime = (long) batchCompleted.batchInfo().processingEndTime().get();
        mapDatas.put("totalDelay",totalDelay);
        mapDatas.put("records",records+"");
        mapDatas.put("processingStratTime",processingStratTime+"");
        mapDatas.put("processingEndTime",processingEndTime+"");
        // 该批次是否出现阻塞
        if(duration * 6 < Long.valueOf(totalDelay) * 1000){
            log.info("流处理程序出现阻塞");
            //发送邮件
        }
        //完成的批次
        finishBatchNum= finishBatchNum + 1;
        mapDatas.put("finish_batchNum",finishBatchNum + "");
        String jsonStr = JSON.toJSONString(mapDatas, SerializerFeature.PrettyFormat);
        jedis.set("batchMessage",jsonStr);
        jedis.expire("batchMessage",3600);
        JedisUtil.returnJedis(jedis);

    }

    @Override
    public void onOutputOperationStarted( StreamingListenerOutputOperationStarted outputOperationStarted) {
    }

    @Override
    public void onOutputOperationCompleted( StreamingListenerOutputOperationCompleted outputOperationCompleted) {
    }


    /**
     * kafka一般采用Direct方式，receiver不使用
     */
    @Override
    public void onReceiverStarted(StreamingListenerReceiverStarted receiverStarted) {
    }

    @Override
    public void onReceiverError(StreamingListenerReceiverError receiverError) {
        Boolean active = receiverError.receiverInfo().active();
        String id = receiverError.receiverInfo().executorId();
        String  message = receiverError.receiverInfo().lastErrorMessage();
        int streamId= receiverError.receiverInfo().streamId();
        mapDatas.put("receiveError","发生错误的executorId为：" + id + ", 错误消息为"+message+",当前流程序是否运行：" + active);
        Jedis jedis = JedisUtil.getJedis();
        String jsonStr= JSON.toJSONString(mapDatas,SerializerFeature.PrettyFormat);
        jedis.set("receiveErrorMsg",jsonStr);
        jedis.expire("receiveErrorMsg",3600);
        JedisUtil.returnJedis(jedis);
    }

    @Override
    public void onReceiverStopped(StreamingListenerReceiverStopped receiverStopped) {
    }
}
