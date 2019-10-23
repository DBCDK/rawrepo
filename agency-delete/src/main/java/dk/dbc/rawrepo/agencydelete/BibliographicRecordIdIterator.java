package dk.dbc.rawrepo.agencydelete;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class BibliographicRecordIdIterator {

    private final int sliceSize;
    private Iterator<String> bibliographicRecordIdSetIterator;
    private int size;

    public BibliographicRecordIdIterator(Set<String> bibliographicRecordIdSetIterator, int sliceSize) {
        this.bibliographicRecordIdSetIterator = bibliographicRecordIdSetIterator.iterator();
        this.size = bibliographicRecordIdSetIterator.size();
        this.sliceSize = sliceSize;
    }

    public int size() {
        return size;
    }

    public boolean hasNext() {
        return bibliographicRecordIdSetIterator.hasNext();
    }

    public Set<String> next() {
        synchronized (this) {
            final Set<String> slice = new HashSet<>(sliceSize);
            while (bibliographicRecordIdSetIterator.hasNext()
                    && slice.size() <= sliceSize) {
                slice.add(bibliographicRecordIdSetIterator.next());
            }
            return slice;
        }
    }
}
