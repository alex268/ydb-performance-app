package tech.ydb.performance;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.OptionSpecBuilder;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class AppConfig {
    public enum Cmd {
        LOAD,
        READ,
        REACT,
    }

    private final static OptionParser PARSER = new OptionParser();

    static {
        PARSER.allowsUnrecognizedOptions();
    }

    private final static OptionSpec<Integer> THREADS = PARSER
            .acceptsAll(Arrays.asList("t", "threads"), "Count of threads")
            .withRequiredArg()
            .ofType(Integer.class)
            .defaultsTo(1);

    private final static OptionSpec<String> TABLE_NAME = PARSER
            .accepts("tablename", "Test table name")
            .withRequiredArg()
            .ofType(String.class)
            .defaultsTo("app_record");

    private final static OptionSpec<Integer> RECORD_COUNT = PARSER
            .acceptsAll(Arrays.asList("rc", "recordcount"), "Count of records in table")
            .withRequiredArg()
            .ofType(Integer.class)
            .defaultsTo(100_000);

    private final static OptionSpec<Integer> RECORD_SIZE = PARSER
            .accepts("recordsize", "Size of record payload")
            .withRequiredArg()
            .ofType(Integer.class)
            .defaultsTo(100);

    private final static OptionSpec<Integer> BATCH_SIZE = PARSER
            .accepts("batchsize")
            .withRequiredArg()
            .ofType(Integer.class)
            .defaultsTo(500);

    private final static OptionSpec<Long> OPERATION_COUNT = PARSER
            .acceptsAll(Arrays.asList("oc", "operationcount"), "Count of operations")
            .withRequiredArg()
            .ofType(Long.class)
            .defaultsTo(100_000l);

    private final static OptionSpecBuilder USE_YDB_SDK_V1 = PARSER
            .accepts("use-sdk-v1");

    private final String endpoint;
    private final Cmd cmd;
    private final String tableName;
    private final int threadsCount;
    private final int recordCount;
    private final int recordSize;
    private final int batchSize;
    private final long operationsCount;
    private final boolean useSdkV1;

    private AppConfig(String endpoint, Cmd cmd, OptionSet options) {
        this.endpoint = endpoint;
        this.cmd = cmd;
        this.tableName = options.valueOf(TABLE_NAME);
        this.threadsCount = options.valueOf(THREADS);
        this.recordCount = options.valueOf(RECORD_COUNT);
        this.recordSize = options.valueOf(RECORD_SIZE);
        this.batchSize = options.valueOf(BATCH_SIZE);
        this.operationsCount = options.valueOf(OPERATION_COUNT);
        this.useSdkV1 = options.has(USE_YDB_SDK_V1);
    }

    public String endpoint() {
        return this.endpoint;
    }

    public Cmd cmd() {
        return this.cmd;
    }

    public String tableName() {
        return this.tableName;
    }

    public int threadsCount() {
        return this.threadsCount;
    }

    public int recordCount() {
        return this.recordCount;
    }

    public int recordSize() {
        return this.recordSize;
    }

    public int batchSize() {
        return this.batchSize;
    }

    public long operationsCount() {
        return this.operationsCount;
    }

    public boolean useSdkV1() {
        return this.useSdkV1;
    }

    public static AppConfig parseArgs(String... args) {
        OptionSet options = PARSER.parse(args);
        List<?> nonOption = options.nonOptionArguments();

        if (nonOption.isEmpty() || nonOption.size() > 2) {

            try {
                System.err.println("Wrong count of free options");
                nonOption.forEach(o -> System.err.println(" >" + o));
                System.err.println("Usage: java -jar ydb-perf-app.jar <options> <endpoint> <cmd>");
                PARSER.printHelpOn(System.err);
                System.exit(1);
            } catch (IOException e) {
                System.exit(2);
            }
        }

        Cmd cmd = Cmd.READ;
        String endpoint = String.valueOf(nonOption.get(0));
        if (nonOption.size() > 1) {
            cmd = Cmd.valueOf(String.valueOf(nonOption.get(1)).toUpperCase());
        }

        return new AppConfig(endpoint, cmd, options);
    }
}
