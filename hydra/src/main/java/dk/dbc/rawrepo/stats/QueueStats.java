/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.stats;

public class QueueStats {

    private String text;
    private int count;
    private String date;

    public QueueStats() {
    }

    public QueueStats(String text, int count, String date) {
        this.text = text;
        this.count = count;
        this.date = date;
    }

    public String getText() {
        return text;
    }

    public void setText(String label) {
        this.text = label;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return "QueueStats{" +
                "text='" + text + '\'' +
                ", count=" + count +
                ", date='" + date + '\'' +
                '}';
    }
}
