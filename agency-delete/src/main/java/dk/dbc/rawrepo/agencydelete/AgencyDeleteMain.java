/*
 * dbc-rawrepo-agency-delete
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-agency-delete.
 *
 * dbc-rawrepo-agency-delete is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-agency-delete is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-agency-delete.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.agencydelete;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.Scanner;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static dk.dbc.rawrepo.agencydelete.CommandLine.integer;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class AgencyDeleteMain {

    private static final Logger log = LoggerFactory.getLogger(AgencyDeleteMain.class);

    private static final String ROLE = "role";
    private static final String DB = "db";
    private static final String MQ = "mq";
    private static final String OPEN_AGENCY = "open-agency";

    public static void main(String[] args) {
        CommandLine commandLine = new AgencyDeleteCommandLine();
        int agencyid;
        try {
            commandLine.parse(args);
            List<String> agencies = commandLine.getExtraArguments();
            if (agencies.size() != 1) {
                throw new IllegalStateException("Only One Agency");
            }
            agencyid = Integer.parseInt(agencies.get(0), 10);
            if (agencyid < 0) {
                throw new NumberFormatException("Positive integer expected");
            }
            if (commandLine.hasOption("debug")) {
                setLogLevel("logback-debug.xml");
            } else {
                setLogLevel("logback-info.xml");
            }

        } catch (IllegalStateException | NumberFormatException ex) {
            System.err.println(commandLine.usage());
            System.exit(1);
            return;
        } catch (JoranException ex) {
            log.error("Exception", ex);
            System.exit(1);
            return;
        }

        String openAgency = null;
        if (commandLine.hasOption(OPEN_AGENCY)) {
            openAgency = (String) commandLine.getOption(OPEN_AGENCY);
        }

        try {
            AgencyDelete agencyDelete = new AgencyDelete((String) commandLine.getOption(DB),
                                                         (String) commandLine.getOption(MQ),
                                                         (Integer) commandLine.getOption("mq-retry-interval"),
                                                         (Integer) commandLine.getOption("mq-retry-count"),
                                                         agencyid, openAgency);
            Set<String> ids = agencyDelete.getIds();
            Set<String> siblingRelations = agencyDelete.getSiblingRelations();
            if (!siblingRelations.isEmpty()) {
                throw new RuntimeException("Cannot remove agency, there's sibling relations to agency");
            }

            System.out.print("Are you sure you want to remove all(" + ids.size() + ") records for agency " + agencyid + " [y/N]? ");
            String line = new Scanner(System.in, "UTF-8").nextLine();
            if (line == null || !line.toLowerCase(Locale.ROOT).startsWith("y")) {
                agencyDelete.close();
                return;
            }

            agencyDelete.begin();
            if (commandLine.hasOption(ROLE)) {
                String role = (String) commandLine.getOption(ROLE);
                agencyDelete.queueRecords(ids, role);
            }
            agencyDelete.deleteRecords(ids);

            agencyDelete.commit();
            agencyDelete.close();
        } catch (Exception ex) {
            log.error(ex.getMessage());
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

    private static class AgencyDeleteCommandLine extends CommandLine {

        @Override
        void setOptions() {
            addOption(DB, "connectstring for database", true, false, string, null);
            addOption(MQ, "connectstring for message queue", false, false, string, null);
            addOption(ROLE, "name of enqueue software (provider: agency-delete)", false, false, string, null);
            addOption(OPEN_AGENCY, "url", false, false, string, null);
            addOption("mq-retry-interval", "ms", false, false, integer, new DefaultInteger(10000));
            addOption("mq-retry-count", "cnt", false, false, integer, new DefaultInteger(60));

            addOption("debug", "turn on debug logging", false, false, null, yes);
        }

        @Override
        String usageCommandLine() {
            return "prog [ options ] agencyid";
        }
    }
}
