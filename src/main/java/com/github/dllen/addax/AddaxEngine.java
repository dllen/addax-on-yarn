package com.github.dllen.addax;

import com.wgzhao.addax.common.exception.AddaxException;
import com.wgzhao.addax.common.statistics.VMInfo;
import com.wgzhao.addax.common.util.Configuration;
import com.wgzhao.addax.core.Engine;
import com.wgzhao.addax.core.util.ConfigParser;
import com.wgzhao.addax.core.util.ConfigurationValidate;
import com.wgzhao.addax.core.util.FrameworkErrorCode;
import com.wgzhao.addax.core.util.container.CoreConstant;
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AddaxEngine extends Engine {

    private final static Logger logger = LoggerFactory.getLogger(AddaxEngine.class);

    private final String reportAddr;

    public AddaxEngine(String reportAddr) {
        this.reportAddr = reportAddr;
    }

    private static void upgradeJobConfig(Configuration configuration) {
        String content = configuration.getString("job.content");
        if (content.startsWith("[")) {
            // get the first element
            List<Map> contentList = configuration.getList(CoreConstant.JOB_CONTENT, Map.class);
            if (contentList != null && contentList.size() > 0) {
                configuration.set("job.content", contentList.get(0));
            }
        }
    }

    private static void validateJob(Configuration conf) {
        final Map content = conf.getMap(CoreConstant.JOB_CONTENT);

        if (content == null || content.isEmpty()) {
            throw AddaxException.asAddaxException(FrameworkErrorCode.JOB_ERROR, "The configuration item '" + CoreConstant.JOB_CONTENT + "' is required");
        }

        if (null == conf.get(CoreConstant.JOB_CONTENT_READER)) {
            throw AddaxException.asAddaxException(FrameworkErrorCode.JOB_ERROR, "The configuration item '" + CoreConstant.JOB_CONTENT_READER + "' is required");
        }

        if (null == conf.get(CoreConstant.JOB_CONTENT_WRITER)) {
            throw AddaxException.asAddaxException(FrameworkErrorCode.JOB_ERROR, "The configuration item '" + CoreConstant.JOB_CONTENT_WRITER + "' is required");
        }

        if (null == conf.get(CoreConstant.JOB_CONTENT_READER_NAME)) {
            throw AddaxException.asAddaxException(FrameworkErrorCode.JOB_ERROR, "The configuration item '" + CoreConstant.JOB_CONTENT_READER_NAME + "' is required");
        }

        if (null == conf.get(CoreConstant.JOB_CONTENT_READER_PARAMETER)) {
            throw AddaxException.asAddaxException(FrameworkErrorCode.JOB_ERROR, "The configuration item '" + CoreConstant.JOB_CONTENT_READER_PARAMETER + "' is required");
        }

        if (null == conf.get(CoreConstant.JOB_CONTENT_WRITER_NAME)) {
            throw AddaxException.asAddaxException(FrameworkErrorCode.JOB_ERROR, "The configuration item '" + CoreConstant.JOB_CONTENT_READER_NAME + "' is required");
        }

        if (null == conf.get(CoreConstant.JOB_CONTENT_WRITER_PARAMETER)) {
            throw AddaxException.asAddaxException(FrameworkErrorCode.JOB_ERROR, "The configuration item '" + CoreConstant.JOB_CONTENT_READER_PARAMETER + "' is required");
        }
    }

    public void start(String jobContent, String jobId) {
        Configuration configuration = Configuration.from(jobContent);
        configuration.set("core.addaxServer.address", reportAddr);

        upgradeJobConfig(configuration);
        validateJob(configuration);
        configuration.merge(Configuration.from(new File(CoreConstant.CONF_PATH)), false);
        String readerPluginName = configuration.getString(CoreConstant.JOB_CONTENT_READER_NAME);
        String writerPluginName = configuration.getString(CoreConstant.JOB_CONTENT_WRITER_NAME);
        String preHandlerName = configuration.getString(CoreConstant.JOB_PRE_HANDLER_PLUGIN_NAME);
        String postHandlerName = configuration.getString(CoreConstant.JOB_POST_HANDLER_PLUGIN_NAME);

        Set<String> pluginList = new HashSet<>();
        pluginList.add(readerPluginName);
        pluginList.add(writerPluginName);

        if (StringUtils.isNotEmpty(preHandlerName)) {
            pluginList.add(preHandlerName);
        }
        if (StringUtils.isNotEmpty(postHandlerName)) {
            pluginList.add(postHandlerName);
        }
        try {
            configuration.merge(ConfigParser.parsePluginConfig(new ArrayList<>(pluginList)), false);
        } catch (Exception e) {
            //吞掉异常，保持log干净。这里message足够。
            logger.warn(String.format("插件[%s,%s]加载失败，1s后重试... Exception:%s ", readerPluginName, writerPluginName, e.getMessage()));
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e1) {
                //
            }
            configuration.merge(ConfigParser.parsePluginConfig(new ArrayList<>(pluginList)), false);
        }

        //打印vmInfo
        VMInfo vmInfo = VMInfo.getVmInfo();
        if (vmInfo != null) {
            logger.info(vmInfo.toString());
        }
        logger.info("设置jobId:{}", jobId);
        configuration.set(CoreConstant.CORE_CONTAINER_JOB_ID, jobId);
        logger.info("\n" + AddaxEngine.filterJobConfiguration(configuration) + "\n");
        logger.debug(configuration.toJSON());
        ConfigurationValidate.doValidate(configuration);
        start(configuration);
    }
}
