/*
 * Copyright (C) 2014 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-bulkqueue
 *
 * dbc-bulkqueue is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-bulkqueue is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.queuebulkload;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.util.StatusPrinter;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.RecordId;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class BulkQueueMain {

    private static final Logger log = LoggerFactory.getLogger(BulkQueueMain.class);

    private static SimpleDateFormat dateFormats[] = {
        new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS"),
        new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    };

    private static Timestamp parseDate(String s) {
        if (s.compareToIgnoreCase("now") == 0) {
            return new Timestamp(new Date().getTime());
        }
        for (SimpleDateFormat dateFormat : dateFormats) {
            try {
                return new Timestamp(dateFormat.parse(s).getTime());
            } catch (ParseException ex) {
            }
        }
        throw new IllegalArgumentException(s + " is not a valid timestamp");
    }

    public static void main(String[] args) {
        CommandLine commandLine = new CommandLineBulkQueue();
        Iterator<RecordId> iterator;
        String db;
        String role;
        Integer commit;
        Integer library;
        BulkQueue bulkQueue;
        try {
            commandLine.parse(args);

            if (commandLine.hasOption("debug")) {
                setLogLevel("logback-debug.xml");
            } else {
                setLogLevel("logback-info.xml");
            }

            db = (String) commandLine.getOption("db");
            role = (String) commandLine.getOption("role");

            library = 870970;
            if (commandLine.hasOption("library")) {
                library = (Integer) commandLine.getOption("library");
            }
            commit = 1000;
            if (commandLine.hasOption("commit")) {
                commit = (Integer) commandLine.getOption("commit");
            }
            bulkQueue = new BulkQueue(db, commit, role);

            if (commandLine.hasOption("stdin")) {
                if (!commandLine.getExtraArguments().isEmpty()) {
                    throw new IllegalStateException("Extra arguments with --stdin does not make sense");
                }
                iterator = new StreamCollection(library).iterator();
            } else if (commandLine.hasOption("file")) {
                if (!commandLine.getExtraArguments().isEmpty()) {
                    throw new IllegalStateException("Extra arguments with --all does not make sense");
                }
                String file = (String) commandLine.getOption("file");
                iterator = new StreamCollection(library, file).iterator();

            } else if (commandLine.hasOption("all")) {
                if (!commandLine.getExtraArguments().isEmpty()) {
                    throw new IllegalStateException("Extra arguments with --all does not make sense");
                }
                iterator = new AllCollection(bulkQueue.connection, library).iterator();
            } else if (commandLine.hasOption("range")) {
                if (commandLine.getExtraArguments().size() != 2) {
                    throw new IllegalStateException("--range takes 2 arguments");
                }
                List<String> extraArguments = commandLine.getExtraArguments();
                iterator = new AllCollection(bulkQueue.connection, library,
                                             parseDate(extraArguments.get(0)), parseDate(extraArguments.get(1))).iterator();

            } else if (!commandLine.getExtraArguments().isEmpty()) {
                if (!commandLine.hasOption("library")) {
                    throw new IllegalStateException("list of ids without --library doesn't make sense");
                }
                ArrayList<RecordId> list = new ArrayList<>(commandLine.getExtraArguments().size());
                for (String id : commandLine.getExtraArguments()) {
                    list.add(new RecordId(id, library));
                }
                iterator = list.iterator();
            } else {
                throw new IllegalStateException("No record id source has been defined");
            }

        } catch (RawRepoException | FileNotFoundException | IllegalStateException | SQLException e) {
            String usage = commandLine.usage();
            System.out.println(usage);
            System.out.println(e.getLocalizedMessage());
            System.exit(1);
            return;
        }

        log.debug("DEBUG");
        log.info("INFO");

        bulkQueue.run(iterator);
    }

    private static void setLogLevel(String file) {
        LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
        try {
            context.reset();
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            InputStream stream = contextClassLoader.getResourceAsStream(file);
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(context);
            configurator.doConfigure(stream); // loads logback file
        } catch (Exception ex) {
            ex.printStackTrace(); // Just in case, so we see a stacktrace
        }
        StatusPrinter.printInCaseOfErrorsOrWarnings(context); // Internal status data is printed in case of warnings or errors.
    }
}

class CommandLineBulkQueue extends CommandLine {

    @Override
    void setOptions() {
        addOption("role", "name of enqueue software (provider)", true, false, string, null);
        addOption("library", "which library to use (default=COMMON_LIBRARY)", false, false, integer, null);
        addOption("stdin", "read ids from stdin", false, false, null, yes);
        addOption("all", "select all ids from the records table in the database", false, false, null, yes);
        addOption("range", "select all ids from the records table in the database, "
                           + "modified between argument 1 & 2 (yyyy-MM-dd hh:mm:ss.SSS)", false, false, null, yes);
        addOption("file", "read ids from file", false, false, string, null);
        addOption("db", "connectstring for database", true, false, string, null);
        addOption("commit", "how often to commit (default=1000)", false, false, integer, null);
        addOption("debug", "turn on debug logging", false, false, null, yes);
    }

    @Override
    String usageCommandLine() {
        return "prog [options] [id ...]";
    }
}
