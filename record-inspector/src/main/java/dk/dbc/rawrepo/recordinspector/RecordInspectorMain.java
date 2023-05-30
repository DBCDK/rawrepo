package dk.dbc.rawrepo.recordinspector;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.vipcore.exception.VipCoreException;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnectorFactory;
import dk.dbc.xmldiff.XmlDiff;
import dk.dbc.xmldiff.XmlDiffTextWriter;
import dk.dbc.xmldiff.XmlDiffWriter;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.xpath.XPathExpressionException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public class RecordInspectorMain {

    public static void main(String[] args) {
        boolean exitOk = true;
        RecordInspectorCommandLine commandLine = new RecordInspectorCommandLine();
        try {
            commandLine.parse(args);
            List<String> arguments = commandLine.getExtraArguments();

            boolean relations = commandLine.hasOption("relations");
            int output = 0;
            if (commandLine.hasOption("text")) {
                output++;
            }
            if (commandLine.hasOption("color")) {
                output++;
            }
            if (commandLine.hasOption("quiet")) {
                output++;
            }

            if (output > 1) {
                throw new IllegalArgumentException("only one of: --color --text --quiet");
            }
            boolean merge = commandLine.hasOption("merge");
            boolean hasMerge = merge && !relations && arguments.size() == 2;
            boolean hasRelationsAndRightArgumentCount = !merge && relations && arguments.size() == 2;
            boolean hasNoRelationsAndRightArgumentCount = !merge && !relations && arguments.size() >= 2 && arguments.size() <= 4;
            if (!hasMerge
                    && !hasRelationsAndRightArgumentCount
                    && !hasNoRelationsAndRightArgumentCount) {
                throw new IllegalArgumentException("Syntax error");
            }

            VipCoreLibraryRulesConnector vipCoreLibraryRulesConnector;
            if (commandLine.hasOption("vipcore")) {
                String vipCoreUrl = (String) commandLine.getOption("vipcore");
                vipCoreLibraryRulesConnector = VipCoreLibraryRulesConnectorFactory.create(vipCoreUrl);
            } else {
                throw new IllegalArgumentException("vipcore url is mandatory");
            }

            int agencyId = Integer.parseInt(arguments.get(0), 10);
            String bibliographicRecordId = arguments.get(1);

            if (commandLine.hasOption("debug")) {
                setLogLevel("logback-debug.xml");
            } else {
                setLogLevel("logback-info.xml");
            }

            try (RecordInspector recordInspector = new RecordInspector((String) commandLine.getOption("db"), vipCoreLibraryRulesConnector)) {
                if (relations) {
                    System.out.println("RELATIONS from me to:");
                    for (String relation : recordInspector.outboundRelations(agencyId, bibliographicRecordId)) {
                        System.out.println(relation);
                    }
                    System.out.println("RELATIONS to me from:");
                    for (String relation : recordInspector.inboundRelations(agencyId, bibliographicRecordId)) {
                        System.out.println(relation);
                    }

                } else {
                    ArrayList<RecordInspector.RecordDescription> timestamps = recordInspector.timestamps(agencyId, bibliographicRecordId);
                    if (merge) {
                        Record record = recordInspector.get(agencyId, bibliographicRecordId);
                        System.out.write(record.getContent());
                    } else if (arguments.size() == 2) {
                        System.out.println("VERSIONS:");
                        for (int i = 0; i < timestamps.size(); i++) {
                            System.out.printf("%2d: %s%n", i, timestamps.get(i).toString());
                        }
                    } else if (arguments.size() == 3) {
                        byte[] content = recordInspector.get(agencyId, bibliographicRecordId, timestamps.get(Integer.parseInt(arguments.get(2), 10)).getTimestamp());
                        if (content == null) {
                            System.out.println("Record is deleted");
                        } else {
                            System.out.write(content);
                        }
                    } else if (arguments.size() == 4) {
                        int leftIndex = Integer.parseInt(arguments.get(2), 10);
                        byte[] left = recordInspector.get(agencyId, bibliographicRecordId, timestamps.get(leftIndex).getTimestamp());
                        int rightIndex = Integer.parseInt(arguments.get(3), 10);
                        byte[] right = recordInspector.get(agencyId, bibliographicRecordId, timestamps.get(rightIndex).getTimestamp());
                        if (left == null) {
                            System.out.println("Left side (" + leftIndex + ") is deleted");
                        }
                        if (right == null) {
                            System.out.println("Right side (" + rightIndex + ") is deleted");
                        }
                        if (left != null && right != null) {
                            XmlDiffWriter writer;
                            if (commandLine.hasOption("color")) {
                                writer = new XmlDiffTextWriter("\u001B[41m", "\u001B[49m", "\u001B[42m", "\u001B[49m", "\u001B[43m", "\u001B[49m");
                            } else if (commandLine.hasOption("text")) {
                                writer = new XmlDiffTextWriter("\u00BB-", "-\u00AB", "\u00BB+", "+\u00AB", "[", "]");
                            } else if (commandLine.hasOption("quiet")) {
                                writer = new XmlDiffWriter();
                            } else {
                                writer = new XmlDiffTextWriter("\u001B[9m", "\u001B[29m", "\u001B[1m", "\u001B[21m", "\u001B[4m", "\u001B[24m");
                            }
                            exitOk = XmlDiff.builder().indent(4).normalize(true).strip(true).trim(true).build()
                                    .compare(new ByteArrayInputStream(left), new ByteArrayInputStream(right), writer);
                            String content = writer.toString();
                            if (!content.isEmpty()) {
                                System.out.println(content);
                            }
                        }
                    }
                    recordInspector.commit();
                }
            } catch (VipCoreException | MarcXMergerException | XPathExpressionException ex) {
                Logger.getLogger(RecordInspectorMain.class.getName()).log(Level.SEVERE, null, ex);
            }
            System.exit(exitOk ? 0 : 1);
        } catch (RawRepoException | JoranException | IOException | IllegalStateException | IllegalArgumentException | SQLException | SAXException e) {
            System.err.println(commandLine.usage());
            System.err.println("Caught: " + e.getClass().getSimpleName() + ": " + e.getLocalizedMessage());
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

    static class RecordInspectorCommandLine extends CommandLine {

        @Override
        void setOptions() {
            addOption("db", "connectstring for database", true, false, string, null);
            addOption("vipcore", "vipcore url", true, false, string, null);
            addOption("relations", "show relations", false, false, null, yes);
            addOption("quiet", "text output", false, false, null, yes);
            addOption("text", "text output", false, false, null, yes);
            addOption("color", "color output", false, false, null, yes);
            addOption("debug", "turn on debug logging", false, false, null, yes);
            addOption("merge", "merge current record (no index)", false, false, null, yes);
        }

        @Override
        String usageCommandLine() {
            return "prog [options] agency id [ index [ index ] ]";
        }

    }

}
