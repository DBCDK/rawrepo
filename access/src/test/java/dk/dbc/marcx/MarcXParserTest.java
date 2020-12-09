/*
 * dbc-rawrepo-access
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-access.
 *
 * dbc-rawrepo-access is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-access is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-access.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.marcx;

import dk.dbc.rawrepo.RecordId;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 * @author DBC {@literal <dbc.dk>}
 */
public class MarcXParserTest {

    public MarcXParserTest() {
    }

    @Test
    public void testParse() throws Exception {
        String parent = MarcXParser.getParent(getFile("marcx-parent.xml"));
        assertThat(parent, is("50155919"));
    }

    private InputStream getFile(String name) {
        return getClass().getClassLoader().getResourceAsStream(name);
    }

    @Test
    public void testAutValues() throws Exception {
        List<RecordId> res = MarcXParser.getAuthorityLinks(getFile("marcxparser/with-authority.xml"));

        assertThat(res, is(Collections.singletonList(new RecordId("68359775", 870979))));
    }

    @Test
    public void testAutNoValues() throws Exception {
        List<RecordId> res = MarcXParser.getAuthorityLinks(getFile("marcxparser/without-authority.xml"));

        assertThat(res, is(Collections.emptyList()));
    }
}
