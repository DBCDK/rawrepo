package dk.dbc.rawrepo.content.service;

import java.util.LinkedList;

/**
 *
 * @author DBC {@literal <dbc.dk>}
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

    protected Pool() {
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
