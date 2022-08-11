package org.shallow;

import org.shallow.internal.config.BrokerConfig;
import org.shallow.logging.InternalLogger;
import org.shallow.logging.InternalLoggerFactory;
import org.shallow.util.StringUtil;

import javax.naming.OperationNotSupportedException;
import java.lang.reflect.Method;
import java.util.Properties;

import static org.shallow.internal.config.ConfigConstants.STAND_ALONE;
import static org.shallow.util.TypeUtil.*;
import static org.shallow.util.TypeUtil.object2String;

public class DefaultApplicationArguments implements ApplicationArguments {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultApplicationArguments.class);

    private final Properties properties;

    public DefaultApplicationArguments(Properties properties) {
        this.properties = properties;
    }

    @Override
    public BrokerConfig config() {
        String isStandAlone = System.getProperty(STAND_ALONE);
        if (!StringUtil.isNullOrEmpty(isStandAlone)) {
            properties.setProperty(STAND_ALONE, isStandAlone);
        }

        BrokerConfig config = BrokerConfig.exchange(properties);

        checkAndPrintConfig(config);
        return config;
    }


    private void checkAndPrintConfig(BrokerConfig config) {
        Method[] methods = BrokerConfig.class.getDeclaredMethods();
        StringBuilder sb = new StringBuilder("Print the broker startup options: \n");
        String option;

        for (Method method : methods) {
            final String name = method.getName();
            if (name.startsWith("get")) {
                option = name.substring(3);
                checkReturnType(method, config, sb, option);
            }

            if (name.startsWith("is")) {
                option = name.substring(2);
                checkReturnType(method, config, sb, option);
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info(sb.toString());
        }
    }

    private void checkReturnType(Method method, BrokerConfig config, StringBuilder sb, String name) {
        String type = method.getReturnType().getSimpleName();
        Object invoke;
        try {
            switch (type) {
                case "int", "Integer" -> invoke = object2Int(method.invoke(config));
                case "long", "Long" -> invoke = object2Long(method.invoke(config));
                case "double", "Double" -> invoke = object2Double(method.invoke(config));
                case "float", "Float" -> invoke = object2Float(method.invoke(config));
                case "boolean", "Boolean" -> invoke = object2Boolean(method.invoke(config));
                case "String" -> invoke = object2String(method.invoke(config));
                default -> throw new OperationNotSupportedException("Not support type");
            }
            sb.append(String.format("\t%s=%s", name, invoke)).append("\n");
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Failed to check config type, type:%s name:%s error:%s", type, name, e));
        }
    }
}
