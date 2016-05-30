/*
 * dbc-rawrepo-rollback
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-rollback.
 *
 * dbc-rawrepo-rollback is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-rollback is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-rollback.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.rollback;

import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class RollBackMain {

    private final static Logger log = LoggerFactory.getLogger( RollBackMain.class );

    private final static String usage = "java rawrepo-roll-back.jar <options> [specific record ids]";

    private static final String OPTION_DB = "db";
    private static final String OPTION_LIBRARY = "library";
    private static final String OPTION_DEBUG = "debug";
    private static final String OPTION_ROLE = "role";
    private static final String OPTION_STATE = "state";
    private static final String OPTION_MATCH = "match";
    private static final String OPTION_TIMESTAMP = "timestamp";
    private static final String OPTION_DRYRUN = "dry-run";

    private static final SimpleDateFormat dateFormats[] = {
        new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" ),
        new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" )
    };

    public static void main( String[] args ) {
        RollBackMain main = new RollBackMain();
        if ( !main.run( args ) ) {
            System.exit( -1 );
        }
    }

    public boolean run( String[] args ) {
        CommandLine options = readOptions( args );
        if ( options == null ) {
            return false;
        }
        if ( options.hasOption( OPTION_DEBUG ) ) {
            ch.qos.logback.classic.Logger root = ( ch.qos.logback.classic.Logger ) LoggerFactory.getLogger( Logger.ROOT_LOGGER_NAME );
            root.setLevel( ch.qos.logback.classic.Level.DEBUG );
        }

        String dbUrl = options.getOptionValue( OPTION_DB );
        String role = options.getOptionValue( OPTION_ROLE );
        int library = Integer.parseInt( options.getOptionValue( OPTION_LIBRARY ) );
        String match = options.getOptionValue( OPTION_MATCH, DateMatch.Match.BeforeOrEqual.name() );
        String state = options.getOptionValue( OPTION_STATE, RollBack.State.Keep.name() );
        String timestamp = options.getOptionValue( OPTION_TIMESTAMP );
        boolean dryrun = options.hasOption( OPTION_DRYRUN );

        String[] records = options.getArgs();

        return rollBackRecords( dbUrl, library, records, timestamp, match, state, role, dryrun );
    }

    private static Connection getConnection( String url ) throws SQLException {

        log.debug( "URL: {}", url );

        Pattern urlPattern = Pattern.compile( "^(jdbc:[^:]*://)?(?:([^:@]*)(?::([^@]*))?@)?((?:([^:/]*)(?::(\\d+))?)(?:/(.*))?)$" );
        final int urlPatternPrefix = 1;
        final int urlPatternUser = 2;
        final int urlPatternPassword = 3;
        final int urlPatternHostPortDb = 4;

        Matcher matcher = urlPattern.matcher( url );
        if ( !matcher.find() ) {
            throw new IllegalArgumentException( url + " Is not a valid jdbc uri" );
        }
        Properties properties = new Properties();
        String jdbc = matcher.group( urlPatternPrefix );
        if ( jdbc == null ) {
            jdbc = "jdbc:postgresql://";
        }
        if ( matcher.group( urlPatternUser ) != null ) {
            String user = matcher.group( urlPatternUser );
            properties.setProperty( "user", user );
        }
        if ( matcher.group( urlPatternPassword ) != null ) {
            String pass = matcher.group( urlPatternPassword );
            properties.setProperty( "password", pass );
        }

        log.debug( "Connecting" );
        String hostPortDb = matcher.group( urlPatternHostPortDb );
        Connection connection = DriverManager.getConnection(jdbc + hostPortDb, properties );
        connection.setAutoCommit( false );
        log.debug( "Connected" );
        return connection;
    }

    private Date parseDate( String s ) {
        for ( SimpleDateFormat dateFormat : dateFormats ) {
            try {
                log.debug( "Parsing {}", s );
                Date date = dateFormat.parse( s );
                log.debug( "Parsed date {}", s );
                return date;
            }
            catch ( ParseException ex ) {
                log.debug( "error parsing date-format " + s );
            }
        }
        throw new IllegalArgumentException( '\'' + s + "' is not a valid timestamp" );
    }

    private boolean rollBackRecords( String dbUrl, int library, String[] records, String timestamp, String match, String state, String role, boolean dryrun ) {

        Date date = parseDate( timestamp );
        DateMatch.Match matchCriteria = DateMatch.Match.valueOf( match );
        RollBack.State stateHandling = RollBack.State.valueOf( state );

        Connection connection = null;
        try {
            connection = getConnection(dbUrl);
            RawRepoDAO dao = RawRepoDAO.builder( connection ).build();
            if ( records.length > 0 ) {
                log.debug( "Rolling back up to {} records to '{}', time matching rule: '{}', library {}, state modification option '{}', queue role '{}'",
                        records.length, timestamp, match, library, state, role );

                RollBack.rollbackRecords( library, Arrays.asList( records ), dao, date, matchCriteria, stateHandling );
            }
            else {
                log.debug( "Rolling back all records to '{}', time matching rule '{}', library {}, state modification option '{}', queue role '{}'",
                        timestamp, match, library, state, role );

                RollBack.rollbackAgency( connection, library, date, matchCriteria, stateHandling, role );
            }
            if ( dryrun ) {
                log.info( "DRY RUN. Not comitting changes." );
                connection.rollback();
            } else {
                log.info( "Comitting changes." );
                connection.commit();
            }
            return true;
        }
        catch ( SQLException | RawRepoException ex ) {
            log.error( "Database error", ex );
            if ( connection != null ) {
                try {
                    connection.rollback();
                    connection.close();
                }
                catch ( SQLException ex2 ) {
                    log.error( "Database closing error", ex2 );
                }
            }
            return false;
        }
    }

    private CommandLine readOptions( String[] args ) {
        Options options = new Options();
        Option help = new Option( "h", "help", false, "Produce help message" );

        @SuppressWarnings( "static-access" )
        Option db = OptionBuilder.withArgName( OPTION_DB ).hasArg().isRequired( true ).
                withDescription( "connectstring for database" ).
                withLongOpt( OPTION_DB ).create( "db" );

        @SuppressWarnings( "static-access" )
        Option library = OptionBuilder.withArgName( OPTION_LIBRARY ).hasArg().isRequired( true ).
                withDescription( "Library agency ID to match records" ).
                withLongOpt( OPTION_LIBRARY ).create( "l" );

        @SuppressWarnings( "static-access" )
        Option date = OptionBuilder.withArgName( OPTION_TIMESTAMP ).hasArg().isRequired( true ).
                withDescription( "Timestamp date to rollback to.\nIn yyyy-MM-dd hh:mm:ss.SSS format, microseconds are optional" ).
                withLongOpt( OPTION_TIMESTAMP ).create( "t" );

        Stream<String> datemap = Arrays.stream( DateMatch.Match.values() ).map( e -> e.getDescription() );
        String dateOptions = datemap.collect( Collectors.joining( ",\n" ) );

        @SuppressWarnings( "static-access" )
        Option match = OptionBuilder.withArgName( OPTION_MATCH ).hasArg().isRequired( false ).
                withDescription( "Criteria to match date matching criteria. Optional. Default is Before or Equal. Supported values are:\n" + dateOptions ).
                withLongOpt( OPTION_MATCH ).create( "m" );

        Stream<String> statemap = Arrays.stream( RollBack.State.values() ).map( e -> e.getDescription() );
        String stateOptions = statemap.collect( Collectors.joining( ",\n" ) );

        @SuppressWarnings( "static-access" )
        Option state = OptionBuilder.withArgName( OPTION_STATE ).hasArg().isRequired( false ).
                withDescription( "How to treat state of matching records. Optional. Default is to keep current state. Supported values are:\n" + stateOptions ).
                withLongOpt( OPTION_STATE ).create( "s" );

        @SuppressWarnings( "static-access" )
        Option role = OptionBuilder.withArgName( OPTION_ROLE ).hasArg().isRequired( false ).
                withDescription( "Enqueue role. Optional. If not specified, modified records will not be enqueued" ).
                withLongOpt( OPTION_ROLE ).create( "r" );

        @SuppressWarnings( "static-access" )
        Option debug = OptionBuilder.withArgName( OPTION_DEBUG ).hasArg( false ).isRequired( false ).
                withDescription( "Enable debug logging" ).
                withLongOpt( OPTION_DEBUG ).create( "d" );

        @SuppressWarnings( "static-access" )
        Option dryrun = OptionBuilder.withArgName( OPTION_DRYRUN ).hasArg( false ).isRequired( false ).
                withDescription( "Run roll back but do not commit changes" ).
                withLongOpt( OPTION_DRYRUN ).create();

        options.addOption( db );
        options.addOption( help );
        options.addOption( library );
        options.addOption( date );
        options.addOption( match );
        options.addOption( state );
        options.addOption( role );
        options.addOption( debug );
        options.addOption( dryrun );

        // Parse options
        HelpFormatter formatter = new HelpFormatter();
        try {
            CommandLineParser parser = new GnuParser();
            CommandLine line = parser.parse( options, args );
            if ( line.hasOption( "h" ) ) {
                formatter.printHelp( usage, options );
                return null;
            }
            else {
                return line;
            }
        }
        catch ( org.apache.commons.cli.ParseException e ) {
            System.err.println( "Parsing options failed.  Reason: " + e.getMessage() );
            formatter.printHelp( usage, options );
        }
        return null;
    }
}
