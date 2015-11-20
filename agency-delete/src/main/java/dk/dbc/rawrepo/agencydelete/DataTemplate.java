/*
 * Copyright (C) 2015 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-agency-delete
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.agencydelete;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
public class DataTemplate {

    private static final Pattern VARIABLE = Pattern.compile("\\$\\{([0-9a-z](?:[-.]?[0-9a-z])*)\\}");
    private ArrayList<String> list; // odd number entries are verbatim, even are properties

    public DataTemplate(String file) throws IOException {

        InputStream stream = getClass().getClassLoader().getResourceAsStream(file);
        if (stream == null) {
            throw new FileNotFoundException(file);
        }
        byte[] bytes = new byte[stream.available()];
        int read = stream.read(bytes);
        if (read != bytes.length) {
            throw new IOException("Shortread expected " + bytes.length + " got " + read);
        }
        this.list = new ArrayList<>();
        String data = new String(bytes, "UTF-8");
        Matcher matcher = VARIABLE.matcher(data);
        int start = 0;
        while (matcher.find()) {
            list.add(data.substring(start, matcher.start()));
            list.add(matcher.group(1));
            start = matcher.end();
        }
        list.add(data.substring(start));
    }

    public String build(Properties props) {
        return build(props, "[UNSET]");
    }

    public String build(Properties props, String defaultValue) {
        StringBuilder sb = new StringBuilder();
        for (Iterator<String> iterator = list.iterator() ; iterator.hasNext() ;) {
            sb.append(iterator.next());
            if (iterator.hasNext()) {
                sb.append(props.getProperty(iterator.next(), defaultValue));
            }
        }
        return sb.toString();
    }

    public String build(String... s) {
        Properties props = new Properties();
        int i = 0;
        while (i < s.length - 1) {
            props.put(s[i], s[i + 1]);
            i = i + 2;
        }
        return build(props, i < s.length ? s[i] : "[UNSET]");
    }
}
