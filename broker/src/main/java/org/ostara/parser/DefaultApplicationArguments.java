package org.ostara.parser;

import static org.ostara.common.util.TypeUtils.object2Boolean;
import static org.ostara.common.util.TypeUtils.object2Double;
import static org.ostara.common.util.TypeUtils.object2Float;
import static org.ostara.common.util.TypeUtils.object2Int;
import static org.ostara.common.util.TypeUtils.object2Long;
import static org.ostara.common.util.TypeUtils.object2String;
import java.lang.reflect.Method;
import java.util.Properties;
import javax.naming.OperationNotSupportedException;
import org.ostara.common.logging.InternalLogger;
import org.ostara.common.logging.InternalLoggerFactory;
import org.ostara.internal.config.ServerConfig;

public class DefaultApplicationArguments implements ApplicationArguments {

    private static final InternalLogger logger = InternalLoggerFactory.getLogger(DefaultApplicationArguments.class);

    private final Properties properties;

    public DefaultApplicationArguments(Properties properties) {
        this.properties = properties;
    }

    @Override
    public ServerConfig config() {
        ServerConfig config = ServerConfig.exchange(properties);

        checkAndPrintConfig(config);
        return config;
    }


    private void checkAndPrintConfig(ServerConfig config) {
        Method[] methods = ServerConfig.class.getDeclaredMethods();
        StringBuilder sb = new StringBuilder("Print the broker startup options: \n");
        String option;

        for (Method method : methods) {
            if (method.getName().equals("getProps")) {
                continue;
            }

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
        logger.info(sb.toString());
    }

    private void checkReturnType(Method method, ServerConfig config, StringBuilder sb, String name) {
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
            throw new IllegalArgumentException(
                    String.format("Failed to check config type, type:%s name:%s error:%s", type, name, e));
        }
    }
}
