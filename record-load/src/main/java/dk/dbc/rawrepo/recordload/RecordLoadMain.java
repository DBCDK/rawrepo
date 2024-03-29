package dk.dbc.rawrepo.recordload;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.vipcore.exception.VipCoreException;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.List;
import org.slf4j.LoggerFactory;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class RecordLoadMain {

    public static void main(String[] args) {
        RecordLoadCommandLine commandLine = new RecordLoadCommandLine();
        try {
            commandLine.parse(args);
            List<String> arguments = commandLine.getExtraArguments();
            boolean delete = commandLine.hasOption("delete");
            boolean set = commandLine.hasOption("set");
            boolean add = commandLine.hasOption("add");
            boolean relation = set || add;
            String mimeType = MarcXChangeMimeType.MARCXCHANGE;
            if (commandLine.hasOption("mimetype")) {
                mimeType = (String) commandLine.getOption("mimetype");
            }

            if (delete && relation
                || set && add
                || !( delete && arguments.size() == 2
                      || relation && arguments.size() >= 2
                      || !delete && !relation && arguments.size() == 3 )) {
                throw new IllegalArgumentException("Commandline syntax error");
            }
            int agencyId = Integer.parseInt(arguments.get(0), 10);
            String bibliographicRecordId = arguments.get(1);
            byte[] content = null;
            if (!delete && !relation) {
                String fileName = arguments.get(2);
                if (fileName.equals("-")) {
                    content = getBytesFromInputStream(System.in);
                } else {
                    try (FileInputStream in = new FileInputStream(fileName)) {
                        content = getBytesFromInputStream(in);
                    } catch (IOException e) {
                        System.err.println("Cannot open file: " + e.getLocalizedMessage());
                        throw e;
                    }
                }
            }
            if (commandLine.hasOption("debug")) {
                setLogLevel("logback-debug.xml");
            } else {
                setLogLevel("logback-info.xml");
            }

            try (RecordLoad recordLoad = new RecordLoad((String) commandLine.getOption("db"))) {
                if (delete) {
                    recordLoad.delete(agencyId, bibliographicRecordId);
                } else if (relation) {
                    List<String> relations = arguments.subList(2, arguments.size());
                    recordLoad.relations(agencyId, bibliographicRecordId, add, relations);
                } else {
                    recordLoad.save(agencyId, bibliographicRecordId, mimeType, content);
                    if (commandLine.hasOption("role")) {
                        String role = (String) commandLine.getOption("role");
                        if (role != null) {
                            recordLoad.enqueue(agencyId, bibliographicRecordId, role);
                        }
                    }
                }
                recordLoad.commit();
            }
        } catch (VipCoreException | RawRepoException | JoranException | IOException | IllegalStateException | IllegalArgumentException | SQLException e) {
            System.err.println(commandLine.usage());
            System.err.println("Caught: " + e.getClass().getName() + ": " + e.getLocalizedMessage());
            System.exit(1);

        }
    }

    private static void setLogLevel(String file) throws JoranException {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        context.reset();
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        InputStream stream = contextClassLoader.getResourceAsStream(file);
        JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);
        configurator.doConfigure(stream); // loads logback file
        StatusPrinter.printInCaseOfErrorsOrWarnings(context); // Internal status data is printed in case of warnings or errors.
    }

    private static byte[] getBytesFromInputStream(InputStream is) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        byte[] buffer = new byte[0xFFFF];
        for (int len ; ( len = is.read(buffer) ) != -1 ;) {
            os.write(buffer, 0, len);
        }
        os.flush();
        return os.toByteArray();
    }

    static class RecordLoadCommandLine extends CommandLine {

        @Override
        void setOptions() {
            addOption("db", "connectstring for database", true, false, string, null);
            addOption("role", "name of enqueue software (provider)", false, false, string, null);
            addOption("delete", "delete record", false, false, null, yes);
            addOption("mimetype", "record mimetype", false, false, string, null);
            addOption("set", "set relations", false, false, null, yes);
            addOption("add", "add relations", false, false, null, yes);
            addOption("debug", "turn on debug logging", false, false, null, yes);
        }

        @Override
        String usageCommandLine() {
            return "prog [options --delete | {--set | --add}] agency id [ recordFile | agencyId:bibliographicRecordId  ...]";
        }

    }
}
