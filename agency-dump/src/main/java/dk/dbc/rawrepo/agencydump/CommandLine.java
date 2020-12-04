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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public abstract class CommandLine {

    public interface ArgumentParser {

        Object parse(String argument, String value);
    }

    public interface DefaultArgument {

        Object parse(String argument);
    }

    private static final Pattern OPTION = Pattern.compile("^--([^=]+)(=(.*))?$", Pattern.MULTILINE | Pattern.DOTALL);

    private final Set<String> requiredOptions;
    private final Set<String> repeatableOptions;
    private final Set<String> knownOptions;
    private final Map<String, ArgumentParser> optionParsers;
    private final Map<String, DefaultArgument> optionFlag;
    private final Map<String, String> optionDescriptions;

    private final Map<String, List<Object>> parsedArguments;
    private final List<String> extraArguments;

    abstract void setOptions();

    abstract String usageCommandLine();

    @SuppressWarnings("OverridableMethodCallInConstructor")
    public CommandLine() {
        requiredOptions = new HashSet<>();
        repeatableOptions = new HashSet<>();
        knownOptions = new HashSet<>();
        optionParsers = new HashMap<>();
        optionFlag = new HashMap<>();
        optionDescriptions = new HashMap<>();
        parsedArguments = new HashMap<>();
        extraArguments = new ArrayList<>();

        setOptions();
    }

    public void parse(String... arguments) {
        for (int i = 0; i < arguments.length; i++) {
            String argument = arguments[i];
            Matcher matcher = OPTION.matcher(argument);
            if (matcher.matches()) {
                String option = matcher.group(1);
                String value = matcher.group(3);
                if (!knownOptions.contains(option)) {
                    throw new IllegalStateException("Unknown option: " + option);
                }
                if (parsedArguments.containsKey(option) && !repeatableOptions.contains(option)) {
                    throw new IllegalStateException("Invalid repeated argument: " + option);
                }
                if (value == null) {
                    if (optionFlag.containsKey(option)) {
                        putOption(option, optionFlag.get(option).parse(option));
                    } else if (optionParsers.containsKey(option)) {
                        throw new IllegalStateException("Option: " + option + " requires an argument");
                    }
                } else {
                    if (optionParsers.containsKey(option)) {
                        putOption(option, optionParsers.get(option).parse(option, value));
                    } else if (optionFlag.containsKey(option)) {
                        throw new IllegalStateException("Option: " + option + " does not take an argument");
                    }
                }
            } else {
                while (i < arguments.length) {
                    extraArguments.add(arguments[i++]);
                }
            }
        }
        Set<String> seen = parsedArguments.keySet();
        Set<String> missing = new HashSet<>(requiredOptions);
        missing.removeAll(seen);
        if (!missing.isEmpty()) {
            StringBuilder text = new StringBuilder();
            for (Iterator<String> it = missing.iterator(); it.hasNext(); ) {
                String option = it.next();
                text.append(option);
                if (it.hasNext()) {
                    text.append(", ");
                }
            }
            throw new IllegalStateException("missing mandatory options: " + text.toString());
        }
    }

    @SuppressWarnings("PMD.UselessParentheses")
    public String usage() {
        String[] options = knownOptions.toArray(new String[0]);
        Arrays.sort(options);
        int max = 0;
        HashMap<String, String> optionsList = new HashMap<>();

        for (String option : options) {
            StringBuilder text = new StringBuilder();
            text.append("--").append(option);
            if (optionParsers.containsKey(option)) {
                if (optionFlag.containsKey(option)) {
                    text.append("(=argument)");
                } else {
                    text.append("=argument");
                }
            }
            String content = text.toString();
            if (content.length() > max) {
                max = content.length();
            }
            optionsList.put(option, content);
        }

        String spaces = new String(new char[max + 7]).replace("\0", " ");

        StringBuilder text = new StringBuilder();

        text.append("Usage: ").append(usageCommandLine()).append("\n\n");
        for (String option : options) {
            text.append((optionsList.get(option) + spaces), 0, max);
            text.append(requiredOptions.contains(option) ? " [1:" : " [0:")
                    .append(repeatableOptions.contains(option) ? "*] " : "1] ");

            String desciption = optionDescriptions.get(option);
            String prefix = "";
            for (String line : desciption.split("\n")) {
                text.append(prefix).append(line).append("\n");
                prefix = spaces;
            }
        }

        return text.toString();
    }

    private void putOption(String key, Object value) {
        if (!parsedArguments.containsKey(key)) {
            parsedArguments.put(key, new ArrayList<>());
        } else if (!repeatableOptions.contains(key)) {
            throw new IllegalArgumentException("Cannot repeat option: " + key);
        }
        parsedArguments.get(key).add(value);
    }

    public boolean hasOption(String key) {
        return parsedArguments.containsKey(key);
    }

    public Object getOption(String key) {
        if (repeatableOptions.contains(key)) {
            throw new IllegalStateException("Asking for one option for a repeatable option: " + key);
        }
        if (!parsedArguments.containsKey(key)) {
            throw new IllegalArgumentException("Option has not been set: " + key);
        }
        return parsedArguments.get(key).get(0);
    }

    public List<Object> getOptions(String key) {
        if (!parsedArguments.containsKey(key)) {
            return new ArrayList<>();
        }
        return parsedArguments.get(key);
    }

    public List<String> getExtraArguments() {
        return extraArguments;
    }

    protected void addOption(String option, String description,
                             boolean required, boolean repeatable,
                             ArgumentParser argumentParser, DefaultArgument defaultArgument) {
        if (optionDescriptions.containsKey(option)) {
            throw new IllegalStateException("Option has already been defined: " + option);
        }
        knownOptions.add(option);
        optionDescriptions.put(option, description);
        if (required) {
            requiredOptions.add(option);
        }
        if (repeatable) {
            repeatableOptions.add(option);
        }
        if (argumentParser != null) {
            optionParsers.put(option, argumentParser);
        }
        if (defaultArgument != null) {
            optionFlag.put(option, defaultArgument);
        }

    }

    public static final ArgumentParser string = (argument, value) -> value;
    public static final DefaultArgument yes = argument -> "yes";
}
