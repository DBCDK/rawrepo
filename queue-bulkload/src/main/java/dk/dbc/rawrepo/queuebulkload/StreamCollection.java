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

import dk.dbc.rawrepo.RecordId;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Scanner;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class StreamCollection implements Iterable<RecordId> {

    InputStream stream;
    private int library;

    public StreamCollection(int library) {
        stream = System.in;
        this.library = library;
    }

    public StreamCollection(int library, String file) throws FileNotFoundException {
        this.stream = new FileInputStream(file);
        this.library = library;
    }

    @Override
    public Iterator<RecordId> iterator() {

        if (stream == null) {
            throw new IllegalStateException("Cannot iterate over input twice");
        }
        final Scanner scanner = new Scanner(stream, "utf-8");
        stream = null;

        return new Iterator<RecordId>() {

            @Override
            public boolean hasNext() {
                return scanner.hasNextLine();
            }

            @Override
            public RecordId next() {
                if (hasNext()) {
                    String nextLine = scanner.nextLine();
                    nextLine = nextLine.replaceAll("[\r\n]*$", "");
                    return new RecordId(nextLine, library);
                }
                throw new NoSuchElementException();
            }

            @Override
            public void remove() {
                throw new IllegalStateException("Cannot remove things from stream");
            }
        };
    }
}
