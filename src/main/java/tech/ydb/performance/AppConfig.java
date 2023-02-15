package tech.ydb.performance;

import java.io.IOException;
import java.util.List;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

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
            .accepts("threads")
            .withRequiredArg()
            .ofType(Integer.class)
            .defaultsTo(1);

    private final static OptionSpec<String> TABLE_NAME = PARSER
            .accepts("tablename")
            .withRequiredArg()
            .ofType(String.class)
            .defaultsTo("app_record");

    private final static OptionSpec<Integer> RECORD_COUNT = PARSER
            .accepts("recordcount")
            .withRequiredArg()
            .ofType(Integer.class)
            .defaultsTo(10_000);

    private final static OptionSpec<Integer> RECORD_SIZE = PARSER
            .accepts("recordsize")
            .withRequiredArg()
            .ofType(Integer.class)
            .defaultsTo(100);

    private final static OptionSpec<Boolean> USE_YDB_SDK_V1 = PARSER
            .accepts("sdk-v1")
            .withRequiredArg()
            .ofType(Boolean.class )
            .defaultsTo(false);

    private final String endpoint;
    private final Cmd cmd;
    private final String tableName;
    private final int threadsCount;
    private final int recordCount;
    private final int recordSize;
    private final boolean useSdkV1;

    private AppConfig(String endpoint, Cmd cmd, OptionSet options) {
        this.endpoint = endpoint;
        this.cmd = cmd;
        this.tableName = options.valueOf(TABLE_NAME);
        this.threadsCount = options.valueOf(THREADS);
        this.recordCount = options.valueOf(RECORD_COUNT);
        this.recordSize = options.valueOf(RECORD_SIZE);
        this.useSdkV1 = options.valueOf(USE_YDB_SDK_V1);
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

    public boolean useSdkV1() {
        return this.useSdkV1;
    }

    public static AppConfig parseArgs(String... args) {
        OptionSet options = PARSER.parse(args);
        List<?> nonOption = options.nonOptionArguments();

        if (nonOption.isEmpty() || nonOption.size() > 2) {
            try {
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
