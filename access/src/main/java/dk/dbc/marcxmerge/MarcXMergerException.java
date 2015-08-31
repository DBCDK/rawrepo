/*
 * Copyright (C) 2014 DBC A/S (http://dbc.dk/)
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
package dk.dbc.marcxmerge;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class MarcXMergerException extends Exception {

    public MarcXMergerException() {
    }

    public MarcXMergerException(String message) {
        super(message);
    }

    public MarcXMergerException(String message, Throwable cause) {
        super(message, cause);
    }

    public MarcXMergerException(Throwable cause) {
        super(cause);
    }

}
