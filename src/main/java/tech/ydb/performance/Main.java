package tech.ydb.performance;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class Main {
    private static final Logger logger = LoggerFactory.getLogger(SimpleApp.class);

    public static void main(String... args) throws IOException {
        logger.info("start app");
        AppConfig config = AppConfig.parseArgs(args);

        if (config.scriptFile() != null) {
            try (ScriptApp app = new ScriptApp(args, config.scriptFile())) {
                app.run();
            } catch (Exception e) {
                logger.error("script app problem", e);
            }
        } else {
            try (SimpleApp app = new SimpleApp(config)) {
                app.run();
            } catch (Exception e) {
                logger.error("simple app problem", e);
            }
        }

        logger.info("app finised");
    }
}
