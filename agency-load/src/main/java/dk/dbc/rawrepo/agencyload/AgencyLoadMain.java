/*
 * Copyright (C) 2015 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-agency-load
 *
 * dbc-rawrepo-agency-load is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-agency-load is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.agencyload;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import dk.dbc.rawrepo.RawRepoException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.xml.parsers.ParserConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
public class AgencyLoadMain {

    private static final Logger log = LoggerFactory.getLogger(AgencyLoadMain.class);

    private static final String PARENT_AGENCIES = "parent-agencies";

    public static void main(String[] args) {
        CommandLine commandLine = new AgencyDumpCommandLine();

        List<Integer> list = new ArrayList<>();
        Integer commonAgency = null;
        String role = null;
        InputStream in = System.in;
        try {
            commandLine.parse(args);
            List<String> agencies = commandLine.getExtraArguments();
            if (agencies.size() == 1) {
                in = new FileInputStream(agencies.get(0));
            } else if (!agencies.isEmpty()) {
                throw new IllegalStateException("Only 1 extra argument allowed");
            }

            if (commandLine.hasOption(PARENT_AGENCIES)) {
                commonAgency = null;
                String parentAgencies = (String) commandLine.getOption(PARENT_AGENCIES);
                for (String agency : parentAgencies.split("[^0-9]+")) {
                    int agencyid = Integer.parseInt(agency, 10);
                    list.add(agencyid);
                    commonAgency = agencyid;
                }
            }
            if (commandLine.hasOption("common")) {
                commonAgency = (Integer) commandLine.getOption("common");
            }

            if (commandLine.hasOption("role")) {
                role = (String) commandLine.getOption("role");
            }

            if (commandLine.hasOption("debug")) {
                setLogLevel("logback-debug.xml");
            } else {
                setLogLevel("logback-info.xml");
            }

        } catch (IllegalStateException | NumberFormatException | FileNotFoundException ex) {
            System.err.println(ex.getMessage());
            System.err.println(commandLine.usage());
            System.exit(1);
            return;
        } catch (JoranException ex) {
            log.error("Exception", ex);
            System.exit(1);
            return;
        }

        boolean useTransaction = !commandLine.hasOption("allow-fail");
        try (AgencyLoad agencyLoad = new AgencyLoad((String) commandLine.getOption("db"),
                                                    list, commonAgency, role, useTransaction)) {
            agencyLoad.timingStart();
            boolean success = true;
            success = agencyLoad.load(in) && success;
            success = agencyLoad.buildParentRelations() && success;
            success = agencyLoad.queue() && success;
            if (success && useTransaction) {
                agencyLoad.commit();
            }
            agencyLoad.close();
            log.info("Done");
            agencyLoad.timingStop();
            agencyLoad.status();

            System.exit(success ? 0 : 1);
        } catch (RawRepoException | SQLException | ParserConfigurationException | SAXException | IOException ex) {
            log.error("Got fatal error: " + ex.getMessage());
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

    private static class AgencyDumpCommandLine extends CommandLine {

        @Override
        void setOptions() {
            addOption("db", "connectstring for database", true, false, string, null);
            addOption(PARENT_AGENCIES, "list of parent-agencied (could be 300000,191919)", false, false, string, null);
            addOption("common", "most common agency (implied by --" + PARENT_AGENCIES + " to last from list). \n" +
                                "\tThis agency holds parent-relations, if record/agency exists", false, false, integer, null);
            addOption("role", "who to put on queue as (could be agency-maintain)", false, false, string, null);
            addOption("debug", "turn on debug logging", false, false, null, yes);
            addOption("allow-fail", "commit even if something fails", false, false, null, yes);
        }

        @Override
        String usageCommandLine() {
            return "prog [ options ] [ file ]";
        }
    }

}
