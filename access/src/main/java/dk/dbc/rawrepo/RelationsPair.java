package dk.dbc.rawrepo;

import java.util.Objects;

public class RelationsPair {

    private RecordId child;
    private RecordId parent;

    public RelationsPair(RecordId child, RecordId parent) {
        this.child = child;
        this.parent = parent;
    }

    public RecordId getParent() {
        return parent;
    }

    public RecordId getChild() {
        return child;
    }

    @Override
    public String toString() {
        return "RelationsPair{" +
                "child=" + child +
                ", parent=" + parent +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelationsPair that = (RelationsPair) o;
        return Objects.equals(child, that.child) && Objects.equals(parent, that.parent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(child, parent);
    }
}
