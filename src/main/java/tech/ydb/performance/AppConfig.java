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
    private final static OptionParser PARSER = new OptionParser();

    static {
        PARSER.allowsUnrecognizedOptions();
    }

    private final static OptionSpec<Integer> THREADS = PARSER
            .accepts("threads")
            .withRequiredArg()
            .ofType(Integer.class )
            .defaultsTo(1);

    private final String endpoint;
    private final int threadsCount;

    private AppConfig(String endpoint, OptionSet options) {
        this.endpoint = endpoint;
        this.threadsCount = options.valueOf(THREADS);
    }

    public String endpoint() {
        return this.endpoint;
    }

    public int threadsCount() {
        return this.threadsCount;
    }

    public static AppConfig parseArgs(String... args) {
        OptionSet options = PARSER.parse(args);
        List<?> nonOption = options.nonOptionArguments();

        if (nonOption.isEmpty() || nonOption.size() > 1) {
            try {
                System.err.println("Usage: java -jar ydb-perf-app.jar <options> <endpoint>");
                PARSER.printHelpOn(System.err);
                System.exit(1);
            } catch (IOException e) {
                System.exit(2);
            }
        }

        String endpoint = String.valueOf(nonOption.get(0));
        return new AppConfig(endpoint, options);
    }
}
