package tech.ydb.performance;


import java.io.File;
import java.io.FileNotFoundException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.performance.api.Metric;



/**
 *
 * @author Aleksandr Gorshenin
 */
public class ScriptApp implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ScriptApp.class);

    private final String[] baseArgs;
    private final Scanner scanner;

    public ScriptApp(String[] args, String scriptFile) throws FileNotFoundException {
        this.baseArgs = args;
        this.scanner = new Scanner(new File(scriptFile));
    }

    public void run() {
        logger.info("read columns...");
        if (!scanner.hasNext()) {
            logger.error("script file must contain column list");
            return;
        }

        String[] columns = scanner.nextLine().split(";");
        for (String column: columns) {
            logger.info("  add column '{}'", column);
        }

        List<double[]> values = new ArrayList<>();

        while (scanner.hasNext()) {
            String[] extraArgs = scanner.nextLine().split(";");
            String[] args = new String[baseArgs.length + extraArgs.length];
            System.arraycopy(baseArgs, 0, args, 0, baseArgs.length);
            System.arraycopy(extraArgs, 0, args, baseArgs.length, extraArgs.length);

            try (SimpleApp app = new SimpleApp(AppConfig.parseArgs(args))) {
                logger.info("run next app");

                Map<String, Double> appMetrics = app.run().stream()
                        .collect(Collectors.toMap(Metric::name, Metric::value));

                double appValues[] = new double[columns.length];

                for (int idx = 0; idx < columns.length; idx += 1) {
                    Double v = appMetrics.remove(columns[idx]);
                    if (v == null) {
                        logger.warn("empty metric {}, put zero", columns[idx]);
                        v = 0d;
                    }
                    appValues[idx] = v;
                }

                for (String metricName: appMetrics.keySet()) {
                    logger.warn("not used metric {}", metricName);
                }

                values.add(appValues);
            }
        }

        StringBuilder csv = new StringBuilder();
        DecimalFormat df = new DecimalFormat("0.#####");

        for (String column: columns) {
            csv.append(column).append(";");
        }
        csv.append("\n");

        for (double[] appValues: values) {
            for (double v: appValues) {
                csv.append(df.format(v)).append(";");
            }
            csv.append("\n");
        }

        logger.info("exported CSV\n{}", csv.toString());
    }

    @Override
    public void close() {
        this.scanner.close();
    }
}
