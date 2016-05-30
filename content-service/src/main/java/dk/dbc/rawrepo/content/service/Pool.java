/*
 * dbc-rawrepo-content-service
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-content-service.
 *
 * dbc-rawrepo-content-service is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-content-service is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-content-service.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo.content.service;

import java.util.LinkedList;

/**
 *
 * @author Morten Bøgeskov (mb@dbc.dk)
 */
public abstract class Pool<T> {

    public static class Element<T> implements AutoCloseable {

        private final Pool<T> pool;
        private final T element;

        private Element(Pool<T> pool, T element) {
            this.pool = pool;
            this.element = element;
        }

        T getElement() {
            return this.element;
        }

        @Override
        public void close() throws Exception {
            pool.putBack(element);
        }

    }

    private final LinkedList<T> list;

    public Pool() {
        this.list = new LinkedList<>();
    }

    public abstract T create();

    public Element<T> take() {
        synchronized (this) {
            if (!list.isEmpty()) {
                return new Element<>(this, list.removeFirst());
            }
        }
        return new Element<>(this, create());
    }

    private void putBack(T element) {
        synchronized (this) {
            list.addFirst(element);
        }
    }

}
