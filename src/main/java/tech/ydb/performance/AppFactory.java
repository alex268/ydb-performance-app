package tech.ydb.performance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.performance.api.Workload;
import tech.ydb.performance.api.YdbRuntime;
import tech.ydb.performance.runtime.YdbRuntimeV1;
import tech.ydb.performance.runtime.YdbRuntimeV2;
import tech.ydb.performance.workload.LoadWorkload;
import tech.ydb.performance.workload.ReadWorkload;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class AppFactory {
    private final static Logger logger = LoggerFactory.getLogger(AppFactory.class);

    private AppFactory() { }

    public static Workload createWorkload(AppConfig config, YdbRuntime runtime) {
        switch (config.cmd()) {
            case LOAD:
                return new LoadWorkload(config, runtime);
            case READ:
                return new ReadWorkload(config, runtime);
            case REACT:
            default:
                throw new RuntimeException("Unimplemented");
        }
    }

    public static YdbRuntime createYdbRuntime(AppConfig config) {
        if (config.useSdkV1()) {
            logger.info("use YDB Java SDK v1");
            return new YdbRuntimeV1(config);
        } else {
            logger.info("use YDB Java SDK v2");
            return new YdbRuntimeV2(config);
        }
    }
}
