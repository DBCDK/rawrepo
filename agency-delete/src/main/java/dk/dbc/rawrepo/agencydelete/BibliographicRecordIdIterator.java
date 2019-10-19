package dk.dbc.rawrepo.agencydelete;

import java.util.Set;
import java.util.stream.Collectors;

public class BibliographicRecordIdIterator {

    private final int sliceSize;
    private Set<String> bibliographicRecordIdSet;
    private int index;

    public BibliographicRecordIdIterator(Set<String> bibliographicRecordIdSet, int sliceSize) {
        this.bibliographicRecordIdSet = bibliographicRecordIdSet;
        this.sliceSize = sliceSize;
        this.index = 0;
    }

    public int size() {
        return bibliographicRecordIdSet.size();
    }

    public boolean hasNext() {
        return index < bibliographicRecordIdSet.size();
    }

    public Set<String> next() {
        synchronized (this) {
            Set<String> slice = bibliographicRecordIdSet.stream()
                    .skip(index)
                    .limit(sliceSize)
                    .collect(Collectors.toSet());

            index += sliceSize;

            return slice;
        }
    }

}
