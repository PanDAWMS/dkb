/**
 * Kafka: external process logger
 *
 * Realization of the external process STDERR reader with logging.
 */

package ru.kiae.dkb.kafka.common.external;

import org.apache.kafka.common.KafkaException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class ExternalProcessLogger implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(ExternalProcessLogger.class);

    private String  command;
    private Process process;

    private BufferedReader STDERR;

    private Pattern lmt_p = Pattern.compile("\\(?(TRACE|DEBUG|INFO|"
                                            + "WARN(?:ING)?|ERROR|==)\\)?");

    private String prev_type = "";

    public ExternalProcessLogger(Process process, String command) {
        this.process = process;
        this.command = command;
        this.STDERR  = new BufferedReader(new InputStreamReader(
                                          process.getErrorStream()));
    }

    public void run() {
        String line;
        try {
            while ((line = STDERR.readLine()) != null) {
                external_log(line);
            }
        } catch (IOException e) { }
    }

    private void external_log(String line) {
        Matcher m = lmt_p.matcher(line);
        String type = "TRACE";
        if (m.lookingAt()) {
            type = m.group(1);
            line = line.replaceFirst("^(.*)?\\(?" + type + "\\)?", "");
        }
        line = "(" + this.command + ")" + line;
        if (type == "==") {
            type = prev_type;
        }
        switch (type) {
            case "TRACE":
                log.trace(line);
                break;
            case "DEBUG":
                log.debug(line);
                break;
            case "INFO":
                log.info(line);
                break;
            case "WARN":
            case "WARNING":
                log.warn(line);
                break;
            case "ERROR":
                log.error(line);
                break;
            default:
                log.trace(line);
        }
        prev_type = type;
    }
}
