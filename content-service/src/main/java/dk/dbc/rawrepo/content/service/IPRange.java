/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.content.service;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class IPRange {

    private static final String N255 = "(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])";
    private static final String IPV4 = N255 + "\\." + N255 + "\\." + N255 + "\\." + N255;
    private static final Pattern IPV4_PATTERN = Pattern.compile("^" + IPV4 + "$", Pattern.DOTALL);
    private static final Pattern IPV4_RANGE_PATTERN = Pattern.compile("^" + IPV4 + "(?:/(3[0-2]|[12]?[0-9])|-" + IPV4 + ")?$", Pattern.DOTALL);

    private final long min, max;

    IPRange(String range) {
        Matcher matcher = IPV4_RANGE_PATTERN.matcher(range);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Not a valid ip range: " + range);
        }
        long num1 = (Long.parseUnsignedLong(matcher.group(1)) << 24) |
                (Long.parseUnsignedLong(matcher.group(2)) << 16) |
                (Long.parseUnsignedLong(matcher.group(3)) << 8) |
                (Long.parseUnsignedLong(matcher.group(4)));
        long minCalc = num1;
        long maxCalc = num1;
        if (matcher.group(5) != null) {
            long prefix = Long.parseLong(matcher.group(5));
            long mask = -1 << (32 - prefix);
            maxCalc = minCalc = num1 & mask;
            maxCalc |= ~mask;
        }
        if (matcher.group(6) != null) {
            long num2 = (Long.parseUnsignedLong(matcher.group(6)) << 24) |
                    (Long.parseUnsignedLong(matcher.group(7)) << 16) |
                    (Long.parseUnsignedLong(matcher.group(8)) << 8) |
                    (Long.parseUnsignedLong(matcher.group(9)));
            maxCalc = Math.max(num1, num2);
            minCalc = Math.min(num1, num2);
        }

        this.min = minCalc;
        this.max = maxCalc;
    }

    boolean inRange(long ip) {
        return ip >= min && ip <= max;
    }

    boolean inRange(String ip) {
        Matcher matcher = IPV4_PATTERN.matcher(ip);
        if (matcher.matches()) {
            return inRange((Long.parseUnsignedLong(matcher.group(1)) << 24) |
                    (Long.parseUnsignedLong(matcher.group(2)) << 16) |
                    (Long.parseUnsignedLong(matcher.group(3)) << 8) |
                    (Long.parseUnsignedLong(matcher.group(4))));
        }
        return false;
    }
}
