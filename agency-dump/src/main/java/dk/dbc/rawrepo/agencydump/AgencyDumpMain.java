/*
 * dbc-rawrepo-agency-dump
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-agency-dump.
 *
 * dbc-rawrepo-agency-dump is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-agency-dump is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-agency-dump.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.agencydump;

import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.RawRepoException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public class AgencyDumpMain {

    private static final Logger log = LoggerFactory.getLogger(AgencyDumpMain.class);

    public static void main(String[] args) {
        CommandLine commandLine = new AgencyDumpCommandLine();
        int agencyid;
        OutputStream out = System.out;
        try {
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
                int cnt = 0;
                for (String option : Arrays.asList("enrichment", "entity")) {
                    if (commandLine.hasOption(option)) {
                        cnt++;
                    }
                }
                if (cnt > 1) {
                    throw new IllegalStateException("Options enrichment, merged, entity are mutually exclusive");
                }

                if (commandLine.hasOption("vipcore") && !commandLine.hasOption("merged")) {
                    throw new IllegalStateException("--vipcore doesn't make sense without --merged");
                }

                if (commandLine.hasOption("output")) {
                    String output = (String) commandLine.getOption("output");
                    out = new FileOutputStream(output);
                }
            } catch (IllegalStateException | NumberFormatException ex) {
                System.err.println(ex.getMessage());
                System.err.println(commandLine.usage());
                System.exit(1);
                return;
            } catch (FileNotFoundException ex) {
                log.error("Error opening output file {}", ex.getMessage());
                System.exit(1);
                return;
            }

            String vipCoreUrl = null;
            if (commandLine.hasOption("vipcore")) {
                vipCoreUrl = (String) commandLine.getOption("vipcore");
            }

            try (AgencyDump agencyDump = new AgencyDump((String) commandLine.getOption("db"), agencyid, vipCoreUrl)) {

                List<String> bibliographicRecordIds;
                if (commandLine.hasOption("enrichment")) {
                    bibliographicRecordIds = agencyDump.getBibliographicRecordIds(AgencyDump.RecordCollection.ENRICHMENT);
                } else if (commandLine.hasOption("entity")) {
                    bibliographicRecordIds = agencyDump.getBibliographicRecordIds(AgencyDump.RecordCollection.ENTITY);
                } else {
                    bibliographicRecordIds = agencyDump.getBibliographicRecordIds(AgencyDump.RecordCollection.ALL);
                }

                agencyDump.dumpRecords(bibliographicRecordIds, out, commandLine.hasOption("merged"));

                log.info("Done");
            } catch (MarcXMergerException | RawRepoException | SQLException | IOException ex) {
                log.error(ex.getMessage());
            }
        } finally {
            try {
                out.close();
            } catch (IOException ex) {
                log.error("Cannot close output: {}", ex.getMessage());
            }
        }
    }

    private static class AgencyDumpCommandLine extends CommandLine {

        @Override
        void setOptions() {
            addOption("db", "connectstring for database", true, false, string, null);
            addOption("output", "name of outputbase (default: stdout)", false, false, string, null);
            addOption("merged", "merge enrichment records", false, false, null, yes);
            addOption("vipcore", "url of vipcore service (default: fallback) only valid if --merged is set", false, false, string, null);
            addOption("enrichment", "enrichment records only", false, false, null, yes);
            addOption("entity", "no enrichment records", false, false, null, yes);
        }

        @Override
        String usageCommandLine() {
            return "prog [ options ] agencyid";
        }
    }
}
