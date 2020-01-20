package com.alibaba.jvm.sandbox.repeater.plugin.core.impl.async;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.jvm.sandbox.repeater.plugin.Constants;
import com.alibaba.jvm.sandbox.repeater.plugin.core.impl.AbstractBroadcaster;
import com.alibaba.jvm.sandbox.repeater.plugin.core.impl.api.DefaultBroadcaster;
import com.alibaba.jvm.sandbox.repeater.plugin.core.serialize.SerializeException;
import com.alibaba.jvm.sandbox.repeater.plugin.core.util.HttpUtil;
import com.alibaba.jvm.sandbox.repeater.plugin.core.util.PropertyUtil;
import com.alibaba.jvm.sandbox.repeater.plugin.core.wrapper.RecordWrapper;
import com.alibaba.jvm.sandbox.repeater.plugin.core.wrapper.SerializerWrapper;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.*;
import com.hellobike.hms.sdk.Hms;
import com.hellobike.hms.sdk.common.SimpleMessage;
import com.hellobike.hms.sdk.common.SimpleMessageBuilder;
import com.hellobike.hms.sdk.producer.HmsCallBack;
import com.hellobike.hms.sdk.producer.SendResponse;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * {@link AsyncBroadcaster} 异步队列方式的消息投递实现
 *
 * @author zhangxin09982
 */
public class AsyncBroadcaster extends AbstractBroadcaster {

    /**
     * 异步方式录制消息投递的topic
     */
    private String broadcastRecordTopic = PropertyUtil.getPropertyOrDefault(Constants.DEFAULT_RECORD_BROADCASTER_TOPIC, "");

    /**
     * 异步方式回放消息投递的topic
     */
    private String broadcastRepeatTopic = PropertyUtil.getPropertyOrDefault(Constants.DEFAULT_REPEAT_BROADCASTER_TOPIC, "");

    /**
     * 回放消息拉取url
     */
    private String pullRecordUrl = PropertyUtil.getPropertyOrDefault(Constants.DEFAULT_CONFIG_DATASOURCE, "");

    public AsyncBroadcaster() {
        super();
    }

    @Override
    protected void broadcastRecord(RecordModel recordModel) {
        try {
            RecordWrapper wrapper = new RecordWrapper(recordModel);
            String body = SerializerWrapper.hessianSerialize(wrapper);
            broadcast(broadcastRecordTopic, body, recordModel.getTraceId());
        } catch (SerializeException e) {
            log.error("broadcast record failed", e);
        } catch (Throwable throwable) {
            log.error("[Error-0000]-broadcast record failed", throwable);
        }
    }

    @Override
    protected void broadcastRepeat(RepeatModel repeatModel) {
        try {
            String body = SerializerWrapper.hessianSerialize(repeatModel);
            broadcast(broadcastRepeatTopic, body, repeatModel.getTraceId());
        } catch (SerializeException e) {
            log.error("broadcast record failed", e);
        } catch (Throwable throwable) {
            log.error("[Error-0000]-broadcast record failed", throwable);
        }
    }

    /**
     * 消息发送
     *
     * @param topic   主题名称
     * @param body    请求内容
     * @param traceId traceId
     */
    private void broadcast(String topic, String body, String traceId) {
        final SimpleMessage message = SimpleMessageBuilder.newInstance()
                .buildPaylod(body.getBytes())
                .buildKey(traceId).build();

        Hms.sendAsync(topic, message, new HmsCallBack() {
            @Override
            public void onException(Throwable exception) {
                log.info("broadcast failed ,traceId={},resp={}", message.getKey(), exception);
            }

            @Override
            public void onResult(SendResponse response) {
                log.info("broadcast success,traceId={},messageId={},resp={},", message.getKey(), response.getMsgId(), response);
            }
        });
    }

    @Override
    public RepeaterResult<RecordModel> pullRecord(RepeatMeta meta) {
        String url;
        if (StringUtils.isEmpty(meta.getDatasource())) {
            url = String.format(pullRecordUrl, meta.getAppName(), meta.getTraceId());
        } else {
            url = meta.getDatasource();
        }
        final HttpUtil.Resp resp = HttpUtil.doGet(url);
        if (!resp.isSuccess() || StringUtils.isEmpty(resp.getBody())) {
            log.info("get repeat data failed, datasource={}, response={}", meta.getDatasource(), resp);
            return RepeaterResult.builder().success(false).message("get repeat data failed").build();
        }
        RepeaterResult<String> pr = JSON.parseObject(resp.getBody(), new TypeReference<RepeaterResult<String>>() {
        });
        if (!pr.isSuccess()) {
            log.info("invalid repeat data found, datasource={}, response={}", meta.getDatasource(), resp);
            return RepeaterResult.builder().success(false).message("repeat data found").build();
        }
        // swap classloader cause this method will be call in target app thread
        ClassLoader swap = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(DefaultBroadcaster.class.getClassLoader());
            RecordWrapper wrapper = SerializerWrapper.hessianDeserialize(pr.getData(), RecordWrapper.class);
            SerializerWrapper.inTimeDeserialize(wrapper.getEntranceInvocation());
            if (meta.isMock() && CollectionUtils.isNotEmpty(wrapper.getSubInvocations())) {
                for (Invocation invocation : wrapper.getSubInvocations()) {
                    SerializerWrapper.inTimeDeserialize(invocation);
                }
            }
            return RepeaterResult.builder().success(true).message("operate success").data(wrapper.reTransform()).build();
        } catch (SerializeException e) {
            return RepeaterResult.builder().success(false).message(e.getMessage()).build();
        } finally {
            Thread.currentThread().setContextClassLoader(swap);
        }
    }
}

