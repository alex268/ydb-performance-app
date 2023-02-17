package tech.ydb.performance;

import tech.ydb.performance.api.Workload;
import tech.ydb.performance.api.YdbRuntime;
import tech.ydb.performance.impl.YdbRuntimeV1;
import tech.ydb.performance.impl.YdbRuntimeV2;
import tech.ydb.performance.workload.LoadWorkload;
import tech.ydb.performance.workload.ReadWorkload;

import static tech.ydb.performance.AppConfig.Cmd.REACT;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class AppFactory {
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
            return new YdbRuntimeV1(config);
        } else {
            return new YdbRuntimeV2(config);
        }
    }
}
