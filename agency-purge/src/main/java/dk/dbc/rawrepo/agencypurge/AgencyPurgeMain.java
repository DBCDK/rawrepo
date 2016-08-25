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
package dk.dbc.rawrepo.agencypurge;

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

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class AgencyPurgeMain {

    private static final Logger log = LoggerFactory.getLogger(AgencyPurgeMain.class);

    public static void main(String[] args) {
        CommandLine commandLine = new AgencyPurgeCommandLine();
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
            System.exit(1);
            return;
        } catch (JoranException ex) {
            log.error("Exception", ex);
            System.exit(1);
            return;
        }

        try {
            AgencyPurge agencyPurge = new AgencyPurge((String) commandLine.getOption("db"), agencyid);

            if (!commandLine.hasOption("force")) {

                int notDeleted = agencyPurge.countRecordsNotDeleted();
                if (notDeleted > 0) {
                    System.out.print("Cannot purge agency " + agencyid + " " + notDeleted + " live (not deleted) records exists");
                }
                int queued = agencyPurge.countQueueEntries();
                if (queued > 0) {
                    System.out.print("Cannot purge agency " + agencyid + " " + queued + " records still on queue");
                }

                if (!commandLine.hasOption("batch")) {
                    System.out.print("Are you sure you want to purge all records for agency " + agencyid + " for prosperity [y/N]? ");
                    String line = new Scanner(System.in, "UTF-8").nextLine();
                    if (line == null || !line.toLowerCase(Locale.ROOT).startsWith("y")) {
                        return;
                    }
                }
            }

            agencyPurge.begin();

            int purgedRecords = agencyPurge.purgeAgency();
            log.info("Records purged: " + purgedRecords);

            agencyPurge.commit();
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

    private static class AgencyPurgeCommandLine extends CommandLine {

        @Override
        void setOptions() {
            addOption("db", "connectstring for database", true, false, string, null);
            addOption("force", "ignore sanity checks", false, false, null, yes);
            addOption("batch", "don't ask for confirmation", false, false, null, yes);

            addOption("debug", "turn on debug logging", false, false, null, yes);
        }

        @Override
        String usageCommandLine() {
            return "prog [ options ] agencyid";
        }
    }
}
