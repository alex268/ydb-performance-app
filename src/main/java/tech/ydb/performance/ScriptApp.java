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

    private static final DecimalFormat DF = new DecimalFormat("0.#####");
    private static final String[] COLUMNS = {
        "REQUESTS_TOTAL_COUNT",
        "REQUESTS_TOTAL_MS",
        "REQUESTS_COUNT_PER_SECOND",
        "GET_SESSION_OK_TOTAL_COUNT",
        "GET_SESSION_OK_TOTAL_MS",
        "GET_SESSION_OK_AVG_MS",
        "GET_SESSION_OK_MIN_MS",
        "GET_SESSION_OK_MAX_MS",
        "READ_DATA_OK_TOTAL_COUNT",
        "READ_DATA_OK_TOTAL_MS",
        "READ_DATA_OK_AVG_MS",
        "READ_DATA_OK_MIN_MS",
        "READ_DATA_OK_MAX_MS"
    };

    private final String[] baseArgs;
    private final Scanner scanner;

    public ScriptApp(String[] args, String scriptFile) throws FileNotFoundException {
        this.baseArgs = args;
        this.scanner = new Scanner(new File(scriptFile));
    }

    public void run() {
        List<Step> steps = new ArrayList<>();

        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            if (line == null || line.isEmpty()) {
                continue;
            }

            if (line.startsWith("label:")) {
                steps.add(new LabelStep(line));
            } else {
                steps.add(new ValuesStep(line));
            }
        }

        steps.forEach(Step::run);

        StringBuilder csv = new StringBuilder();

        for (String column: COLUMNS) {
            csv.append(column).append(";");
        }
        csv.append("\n");
        steps.forEach(s -> s.write(csv));

        logger.info("exported CSV\n{}", csv.toString());
    }

    @Override
    public void close() {
        this.scanner.close();
    }

    private interface Step extends Runnable {
        void write(StringBuilder sb);
    }

    private class LabelStep implements Step {
        private final String label;

        public LabelStep(String line) {
            this.label = line.substring(6); // remove prefix label:
        }

        @Override
        public void write(StringBuilder sb) {
            sb.append(label).append("\n");
        }

        @Override
        public void run() {
        }
    }

    private class ValuesStep implements Step {
        private final double[] values = new double[COLUMNS.length];
        private final String[] args;

        public ValuesStep(String line) {
            String[] extraArgs = line.split("\\s+");

            this.args = new String[baseArgs.length + extraArgs.length];
            System.arraycopy(baseArgs, 0, args, 0, baseArgs.length);
            System.arraycopy(extraArgs, 0, args, baseArgs.length, extraArgs.length);
        }

        @Override
        public void run() {
            try (SimpleApp app = new SimpleApp(AppConfig.parseArgs(args))) {
                logger.info("run next app");

                Map<String, Double> appMetrics = app.run().stream()
                        .collect(Collectors.toMap(Metric::name, Metric::value));

                for (int idx = 0; idx < COLUMNS.length; idx += 1) {
                    Double v = appMetrics.remove(COLUMNS[idx]);
                    if (v == null) {
                        logger.warn("empty metric {}, put zero", COLUMNS[idx]);
                        v = 0d;
                    }
                    values[idx] = v;
                }

                for (String metricName: appMetrics.keySet()) {
                    logger.warn("not used metric {}", metricName);
                }
            }
        }

        @Override
        public void write(StringBuilder sb) {
            for (double v: values) {
                sb.append(DF.format(v)).append(";");
            }
            sb.append("\n");
        }
    }
}
