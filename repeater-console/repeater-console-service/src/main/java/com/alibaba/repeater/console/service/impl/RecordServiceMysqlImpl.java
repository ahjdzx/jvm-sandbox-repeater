package com.alibaba.repeater.console.service.impl;

import com.alibaba.jvm.sandbox.repeater.plugin.core.serialize.SerializeException;
import com.alibaba.jvm.sandbox.repeater.plugin.core.wrapper.RecordWrapper;
import com.alibaba.jvm.sandbox.repeater.plugin.core.wrapper.SerializerWrapper;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.RecordModel;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.RepeatModel;
import com.alibaba.jvm.sandbox.repeater.plugin.domain.RepeaterResult;
import com.alibaba.repeater.console.dal.mapper.RecordMapper;
import com.alibaba.repeater.console.dal.model.Record;
import com.alibaba.repeater.console.service.RecordService;
import com.alibaba.repeater.console.service.util.ConvertUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link RecordServiceMysqlImpl} 使用mysql实现存储
 * <p>
 *
 * @author zhaoyb1990
 */
@Service("recordServiceMysql")
public class RecordServiceMysqlImpl extends AbstractRecordService implements RecordService {

    @Resource
    private RecordMapper recordMapper;

    /**
     * key:repeatId
     */
    private volatile Map<String, Record> repeatCache = new ConcurrentHashMap<String, Record>(4096);

    /**
     * key:repeatId
     */
    private volatile Map<String, RepeatModel> repeatModelCache = new ConcurrentHashMap<String, RepeatModel>(4096);

    @Override
    public RepeaterResult<String> saveRecord(String body) {
        try {
            RecordWrapper wrapper = SerializerWrapper.hessianDeserialize(body, RecordWrapper.class);
            if (wrapper == null || StringUtils.isEmpty(wrapper.getAppName())) {
                return RepeaterResult.builder().success(false).message("invalid request").build();
            }
            Record record = ConvertUtil.convertWrapper(wrapper, body);
            recordMapper.insert(record);
            return RepeaterResult.builder().success(true).message("operate success").data("-/-").build();
        } catch (Throwable throwable) {
            return RepeaterResult.builder().success(false).message(throwable.getMessage()).build();
        }
    }

    @Override
    public RepeaterResult<String> saveRepeat(String body) {
        try {
            RepeatModel rm = SerializerWrapper.hessianDeserialize(body, RepeatModel.class);
            Record record = repeatCache.remove(rm.getRepeatId());
            if (record == null) {
                return RepeaterResult.builder().success(false).message("invalid repeatId:" + rm.getRepeatId()).build();
            }
            RecordWrapper wrapper = SerializerWrapper.hessianDeserialize(record.getWrapperRecord(), RecordWrapper.class);
            rm.setOriginResponse(SerializerWrapper.hessianDeserialize(wrapper.getEntranceInvocation().getResponseSerialized()));
            repeatModelCache.put(rm.getRepeatId(), rm);
        } catch (Throwable throwable) {
            return RepeaterResult.builder().success(false).message(throwable.getMessage()).build();
        }
        return RepeaterResult.builder().success(true).message("operate success").data("-/-").build();
    }

    @Override
    public RepeaterResult<String> get(String appName, String traceId) {
        Record record = recordMapper.selectByAppNameAndTraceId(appName, traceId);
        if (record == null) {
            return RepeaterResult.builder().success(false).message("data not exits").build();
        }
        try {
            SerializerWrapper.hessianDeserialize(record.getWrapperRecord(), RecordModel.class);
        } catch (SerializeException e) {
            e.printStackTrace();
        }
        return RepeaterResult.builder().success(true).message("operate success").data(record.getWrapperRecord()).build();
    }

    @Override
    public RepeaterResult<String> repeat(String appName, String traceId, String repeatId) {
        final Record record = recordMapper.selectByAppNameAndTraceId(appName, traceId);
        if (record == null) {
            return RepeaterResult.builder().success(false).message("data does not exist").build();
        }
        RepeaterResult<String> pr = repeat(record, repeatId);
        if (pr.isSuccess()) {
            repeatCache.put(pr.getData(), record);
        }
        return pr;
    }

    @Override
    public RepeaterResult<RepeatModel> callback(String repeatId) {
        if (repeatCache.containsKey(repeatId)) {
            return RepeaterResult.builder().success(true).message("operate is going on").build();
        }
        RepeatModel rm = repeatModelCache.get(repeatId);
        // 进行diff
        if (rm == null) {
            return RepeaterResult.builder().success(true).message("operate success").data(rm).build();
        }
        return RepeaterResult.builder().success(true).message("operate success").data(rm).build();
    }
}
