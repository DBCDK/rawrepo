package dk.dbc.marcrecord;

import dk.dbc.common.records.ExpandCommonMarcRecord;
import dk.dbc.common.records.MarcRecordExpandException;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;

import java.util.HashMap;
import java.util.Map;

public class ExpandCommonRecord {
    public static void expandRecord(Record expandableRecord, Map<String, Record> authorityRecords)
            throws MarcRecordExpandException, MarcReaderException {
        expandRecord(expandableRecord, authorityRecords, false);
    }

    /**
     * This function performs authority expansion on a rawrepo Record.
     *
     * @param expandableRecord The record which should be expanded
     * @param authorityRecords List of authority records to be used for expanding
     * @param keepAutFields    If true the  *5 and *6 fields remains in the output record
     * @throws RawRepoException When expansion fails (usually due to missing authority record)
     */
    public static void expandRecord(Record expandableRecord, Map<String, Record> authorityRecords, boolean keepAutFields)
            throws MarcRecordExpandException, MarcReaderException {
        final Map<String, byte[]> authorityContent = new HashMap<>();
        for (Map.Entry<String, Record> entry : authorityRecords.entrySet()) {
            authorityContent.put(entry.getKey(), entry.getValue().getContent());
        }

        final byte[] content = ExpandCommonMarcRecord.expandRecord(expandableRecord.getContent(), authorityContent, keepAutFields);
        expandableRecord.setContent(content);
    }


}
