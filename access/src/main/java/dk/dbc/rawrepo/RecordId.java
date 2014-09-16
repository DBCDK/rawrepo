/*
 * Copyright (C) 2014 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo-commons
 *
 * dbc-rawrepo-commons is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-commons is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo;

import java.util.Objects;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class RecordId {

    protected String id;
    protected int library;

    public RecordId() {
    }

    public RecordId(String id, int library) {
        if (id == null) {
            throw new IllegalArgumentException("Id cannot be 'null'");
        }
        this.id = id;
        this.library = library;
    }

    public String getId() {
        return id;
    }

    public int getLibrary() {
        return library;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 53 * hash + Objects.hashCode(this.id);
        hash = 53 * hash + this.library;
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final RecordId other = (RecordId) obj;
        if (!Objects.equals(this.id, other.id)) {
            return false;
        }
        if (this.library != other.library) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return "RecordId{" + "id=" + id + ", library=" + library + '}';
    }

}
