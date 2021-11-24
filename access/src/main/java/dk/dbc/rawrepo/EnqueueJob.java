package dk.dbc.rawrepo;

public class EnqueueJob {

    private RecordId job;
    private String provider;
    private boolean changed;
    private boolean leaf;
    private int priority = 1000;

    public RecordId getJob() {
        return job;
    }

    public String getProvider() {
        return provider;
    }

    public boolean isChanged() {
        return changed;
    }

    public boolean isLeaf() {
        return leaf;
    }

    public int getPriority() {
        return priority;
    }

    public EnqueueJob withRecordId(RecordId recordId) {
        this.job = recordId;
        return this;
    }

    public EnqueueJob withProvider(String provider) {
        this.provider = provider;
        return this;
    }

    public EnqueueJob withChanged(boolean changed) {
        this.changed = changed;
        return this;
    }

    public EnqueueJob withLeaf(boolean leaf) {
        this.leaf = leaf;
        return this;
    }

    public EnqueueJob withPriority(int priority) {
        this.priority = priority;
        return this;
    }
}
