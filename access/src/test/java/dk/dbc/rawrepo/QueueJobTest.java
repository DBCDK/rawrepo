/*
 * Copyright (C) 2017 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo
 *
 * dbc-rawrepo is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo;

import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.junit.Assert.*;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class QueueJobTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    public void testJson() throws Exception {
        QueueJob queueJob = new QueueJob("abc", 123);
        String expected = OBJECT_MAPPER.writeValueAsString(queueJob);
        System.out.println("expected = " + expected);
        String actual = queueJob.toJSON();
        assertEquals(expected, actual);

        queueJob = QueueJob.fromJSON(expected);
        actual = queueJob.toJSON();
        assertEquals(expected, actual);

    }

    @Test
    public void testJsonErrorNotNull() throws Exception {
        QueueJob queueJob = new QueueJob("abc", 123);
        queueJob.setError("WOOT WOOT");
        String expected = OBJECT_MAPPER.writeValueAsString(queueJob);
        System.out.println("expected = " + expected);
        String actual = queueJob.toJSON();
        assertEquals(expected, actual);

        queueJob = QueueJob.fromJSON(expected);
        actual = queueJob.toJSON();
        assertEquals(expected, actual);

    }

}
