/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.marcrecord;


import dk.dbc.common.records.*;
import dk.dbc.common.records.utils.RecordContentTransformer;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.UnsupportedEncodingException;
import java.util.*;

public class ExpandCommonMarcRecord {
    private static final XLogger logger = XLoggerFactory.getXLogger(ExpandCommonMarcRecord.class);
    private static final List<String> AUTHORITY_FIELD_LIST = Arrays.asList("100", "600", "700");

    /**
     * This function performs authority expansion on a rawrepo Record.
     *
     * @param expandableRecord The record which should be expanded
     * @param authorityRecords List of authority records to be used for expanding
     * @throws RawRepoException When expansion fails (usually due to missing authority record)
     */
    public static void expandRecord(Record expandableRecord, Map<String, Record> authorityRecords, boolean keepAutFields) throws RawRepoException {
        try {
            MarcRecord commonMarcRecord = RecordContentTransformer.decodeRecord(expandableRecord.getContent());

            Map<String, MarcRecord> authorityMarcRecords = new HashMap<>();
            for (Map.Entry<String, Record> entry : authorityRecords.entrySet()) {
                authorityMarcRecords.put(entry.getKey(), RecordContentTransformer.decodeRecord(entry.getValue().getContent()));
            }

            MarcRecord expandedMarcRecord = doExpand(commonMarcRecord, authorityMarcRecords, keepAutFields);

            sortFields(expandedMarcRecord);

            expandableRecord.setContent(RecordContentTransformer.encodeRecord(expandedMarcRecord));
        } catch (JAXBException | UnsupportedEncodingException e) {
            throw new RawRepoException(e);
        }
    }

    public static void expandRecord(Record expandableRecord, Map<String, Record> authorityRecords) throws RawRepoException {
        expandRecord(expandableRecord, authorityRecords, false);
    }

    /**
     * The function takes a set of  records and return a common marc record expanded with authority fields (if any)
     *
     * @param records map containing a common record and x amount of authority records
     * @return a single common record expanded with authority data
     * @throws RawRepoException if the collection doesn't contain the necessary records
     */
    public static MarcRecord expandMarcRecord(Map<String, MarcRecord> records, boolean keepAutFields) throws RawRepoException {
        MarcRecord commonRecord = null;
        Map<String, MarcRecord> authorityRecords = new HashMap<>();

        logger.info("Record collection contains:");
        // Key is the recordId and value is the record. AgencyId have to be found in the record
        for (Map.Entry<String, MarcRecord> entry : records.entrySet()) {
            MarcRecordReader reader = new MarcRecordReader(entry.getValue());
            String recordId = entry.getKey();
            String agencyId = reader.getAgencyId();
            logger.info("{}:{}", recordId, agencyId);
            if (Arrays.asList("870970", "870971").contains(agencyId)) {
                commonRecord = new MarcRecord(entry.getValue());
            } else if ("870979".equals(agencyId)) {
                authorityRecords.put(recordId, new MarcRecord(entry.getValue()));
            }
        }

        if (commonRecord == null) {
            throw new RawRepoException("The record collection doesn't contain a common record");
        }

        return doExpand(commonRecord, authorityRecords, keepAutFields);
    }

    public static MarcRecord expandMarcRecord(Map<String, MarcRecord> records) throws UnsupportedEncodingException, RawRepoException {
        return expandMarcRecord(records, false);
    }

    private static MarcRecord doExpand(MarcRecord commonRecord, Map<String, MarcRecord> authorityRecords, boolean keepAutFields) throws RawRepoException {
        MarcRecord expandedRecord = new MarcRecord();
        /*
         * Okay, here are (some) of the rules for expanding with auth records:
         * Fields that can contain AUT are: 100, 600, 700
         * AUT reference are located in *5 and *6
         *
         * A field points to AUT data if:
         * Field name is either 100, 600 or 700
         * And contains subfields *5 and *6
         *
         * Rules for expanding are:
         * Remove *5 and *6
         * Add all subfields from AUT record field 100 at the same location as *5
         * If AUT record contains field 400 or 500 then add that field as well to the expanded record but as field 900
         */
        MarcRecordReader reader = new MarcRecordReader(commonRecord);
        handleNonRepeatableField(reader.getField("100"), expandedRecord, authorityRecords, keepAutFields);
        handleRepeatableField(reader.getFieldAll("600"), expandedRecord, authorityRecords, keepAutFields);
        handleRepeatableField(reader.getFieldAll("700"), expandedRecord, authorityRecords, keepAutFields);

        for (MarcField field : commonRecord.getFields()) {
            if (!AUTHORITY_FIELD_LIST.contains(field.getName())) {
                expandedRecord.getFields().add(new MarcField(field));
            }
        }

        return expandedRecord;
    }

    private static void handleRepeatableField(List<MarcField> fields, MarcRecord expandedRecord, Map<String, MarcRecord> authorityRecords, boolean keepAutFields) throws RawRepoException {
        Integer authIndicator = 0;
        for (MarcField field : fields) {
            MarcFieldReader fieldReader = new MarcFieldReader(field);
            if (fieldReader.hasSubfield("å")) {
                Integer indicator = Integer.parseInt(fieldReader.getValue("å"));
                if (indicator > authIndicator) {
                    authIndicator = indicator;
                }
            }
        }
        authIndicator++;

        for (MarcField field : fields) {
            MarcFieldReader fieldReader = new MarcFieldReader(field);

            if (fieldReader.hasSubfield("5") && fieldReader.hasSubfield("6")) {
                String authRecordId = fieldReader.getValue("6");

                MarcRecord authRecord = authorityRecords.get(authRecordId);

                if (authRecord == null) {
                    String message = String.format("Autoritetsposten '%s' blev ikke fundet i forbindelse med ekspandering af fællesskabsposten", authRecordId);
                    logger.error(message);
                    throw new RawRepoException(message);
                }

                MarcField expandedField = new MarcField(field);
                MarcRecordReader authRecordReader = new MarcRecordReader(authRecord);

                addMainField(expandedField, new MarcField(authRecordReader.getField("100")), keepAutFields);

                if (authRecordReader.hasField("400") || authRecordReader.hasField("500")) {
                    String indicator = field.getName();
                    expandedField.getSubfields().add(0, new MarcSubField("å", authIndicator.toString()));
                    indicator += "/" + authIndicator.toString();
                    addAdditionalFields(expandedRecord, authRecordReader.getFieldAll("400"), indicator);
                    addAdditionalFields(expandedRecord, authRecordReader.getFieldAll("500"), indicator);

                    authIndicator++;
                }

                expandedRecord.getFields().add(new MarcField(expandedField));
            } else {
                expandedRecord.getFields().add(new MarcField(field));
            }
        }
    }

    private static void handleNonRepeatableField(MarcField field, MarcRecord expandedRecord, Map<String, MarcRecord> authorityRecords, boolean keepAutFields) throws RawRepoException {
        MarcFieldReader fieldReader = new MarcFieldReader(field);
        if (field != null) {
            if (fieldReader.hasSubfield("5") && fieldReader.hasSubfield("6")) {
                String authRecordId = fieldReader.getValue("6");

                MarcRecord authRecord = authorityRecords.get(authRecordId);

                if (authRecord == null) {
                    String message = String.format("Autoritetsposten '%s' blev ikke fundet i forbindelse med ekspandering af fællesskabsposten", authRecordId);
                    logger.error(message);
                    throw new RawRepoException(message);
                }

                MarcField expandedField = new MarcField(field);
                MarcRecordReader authRecordReader = new MarcRecordReader(authRecord);

                addMainField(expandedField, new MarcField(authRecordReader.getField("100")), keepAutFields);

                expandedRecord.getFields().add(new MarcField(expandedField));

                addAdditionalFields(expandedRecord, authRecordReader.getFieldAll("400"), field.getName());
                addAdditionalFields(expandedRecord, authRecordReader.getFieldAll("500"), field.getName());
            } else {
                expandedRecord.getFields().add(new MarcField(field));
            }
        }
    }

    private static void addMainField(MarcField field, MarcField authField, boolean keepAutFields) {
        // Find the index of where the AUT reference subfields are in the field
        // We need to add the AUT content at that location
        Integer authSubfieldIndex = 0;
        for (int i = 0; i < field.getSubfields().size(); i++) {
            if (field.getSubfields().get(i).getName().equals("5")) {
                authSubfieldIndex = i;
                break;
            }
        }

        if (keepAutFields) {
            // If we are keeping *5 and *6 then move the aut data 2 fields "back"
            authSubfieldIndex += 2;
        } else {
            MarcFieldWriter expandedFieldWriter = new MarcFieldWriter(field);
            expandedFieldWriter.removeSubfield("5");
            expandedFieldWriter.removeSubfield("6");
        }
        field.setIndicator("00");
        for (MarcSubField authSubfield : authField.getSubfields()) {
            field.getSubfields().add(authSubfieldIndex++, new MarcSubField(authSubfield));
        }
    }

    private static void addAdditionalFields(MarcRecord record, List<MarcField> authFields, String indicator) {
        for (MarcField field : authFields) {
            MarcField additionalField = new MarcField("900", "00");
            additionalField.setSubfields(field.getSubfields());
            additionalField.getSubfields().add(new MarcSubField("z", indicator));
            record.getFields().add(additionalField);
        }
    }

    private static void sortFields(MarcRecord record) {
        // First sort by field name then sort by subfield å
        Collections.sort(record.getFields(), new Comparator<MarcField>() {
            public int compare(MarcField m1, MarcField m2) {
                if (m1.getName().equals(m2.getName())) {
                    MarcFieldReader fr1 = new MarcFieldReader(m1);
                    MarcFieldReader fr2 = new MarcFieldReader(m2);

                    String aa1 = fr1.hasSubfield("å") ? fr1.getValue("å") : "";
                    String aa2 = fr2.hasSubfield("å") ? fr2.getValue("å") : "";

                    return aa1.compareTo(aa2);
                }

                return m1.getName().compareTo(m2.getName());
            }
        });
    }

}