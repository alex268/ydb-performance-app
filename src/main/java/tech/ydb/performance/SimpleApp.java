package tech.ydb.performance;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class SimpleApp implements Runnable, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(SimpleApp.class);

    public static void main(String... args) throws IOException {
        AppConfig config = AppConfig.parseArgs(args);
        try (SimpleApp app = new SimpleApp(config)) {
            app.run();
        } catch (Exception e) {
            logger.error("app problem", e);
        }
        logger.info("app finised");
    }

    private final AppConfig config;

    public SimpleApp(AppConfig config) {
        this.config = config;
    }

    @Override
    public void run() {

    }

    @Override
    public void close() {
    }
}
