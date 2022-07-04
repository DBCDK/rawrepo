
package dk.dbc.marcrecord;


import dk.dbc.common.records.MarcField;
import dk.dbc.common.records.MarcFieldReader;
import dk.dbc.common.records.MarcFieldWriter;
import dk.dbc.common.records.MarcRecord;
import dk.dbc.common.records.MarcRecordReader;
import dk.dbc.common.records.MarcSubField;
import dk.dbc.common.records.utils.RecordContentTransformer;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.util.Stopwatch;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.xml.bind.JAXBException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ExpandCommonMarcRecord {
    private static final XLogger logger = XLoggerFactory.getXLogger(ExpandCommonMarcRecord.class);
    public static final List<String> AUTHORITY_FIELD_LIST = Arrays.asList("100", "110", "600", "610", "700", "710", "770", "780", "845", "846");

    /**
     * This function performs authority expansion on a rawrepo Record.
     *
     * @param expandableRecord The record which should be expanded
     * @param authorityRecords List of authority records to be used for expanding
     * @param keepAutFields    If true the  *5 and *6 fields remains in the output record
     * @throws RawRepoException When expansion fails (usually due to missing authority record)
     */
    public static void expandRecord(Record expandableRecord, Map<String, Record> authorityRecords, boolean keepAutFields) throws RawRepoException {
        final Map<String, byte[]> authorityContent = new HashMap<>();
        for (Map.Entry<String, Record> entry : authorityRecords.entrySet()) {
            authorityContent.put(entry.getKey(), entry.getValue().getContent());
        }

        final byte[] content = expandRecord(expandableRecord.getContent(), authorityContent, keepAutFields);

        expandableRecord.setContent(content);
    }

    /**
     * This function performs authority expansion on a rawrepo Record.
     *
     * @param content          The record content which should be expanded
     * @param authorityContent List of authority record content to be used for expanding
     * @param keepAutFields    If true the  *5 and *6 fields remains in the output record
     * @throws RawRepoException When expansion fails (usually due to missing authority record)
     */
    public static byte[] expandRecord(byte[] content, Map<String, byte[]> authorityContent, boolean keepAutFields) throws RawRepoException {
        try {
            final Stopwatch stopWatch = new Stopwatch();
            final MarcRecord commonMarcRecord = RecordContentTransformer.decodeRecord(content);
            logger.debug("Stopwatch - {} took {} ms", "RecordContentTransformer.decodeRecord(common)", stopWatch.getElapsedTime(TimeUnit.MILLISECONDS));
            stopWatch.reset();

            final Map<String, MarcRecord> authorityMarcRecords = new HashMap<>();
            for (Map.Entry<String, byte[]> entry : authorityContent.entrySet()) {
                authorityMarcRecords.put(entry.getKey(), RecordContentTransformer.decodeRecord(entry.getValue()));
                logger.debug("Stopwatch - {} took {} ms", "RecordContentTransformer.decodeRecord(loop)", stopWatch.getElapsedTime(TimeUnit.MILLISECONDS));
                stopWatch.reset();
            }

            final MarcRecord expandedMarcRecord = doExpand(commonMarcRecord, authorityMarcRecords, keepAutFields);
            logger.debug("Stopwatch - {} took {} ms", "doExpand", stopWatch.getElapsedTime(TimeUnit.MILLISECONDS));
            stopWatch.reset();

            logger.debug("Stopwatch - {} took {} ms", "sortFields", stopWatch.getElapsedTime(TimeUnit.MILLISECONDS));
            stopWatch.reset();

            logger.debug("Stopwatch - {} took {} ms", "RecordContentTransformer.encodeRecord", stopWatch.getElapsedTime(TimeUnit.MILLISECONDS));
            stopWatch.reset();
            return RecordContentTransformer.encodeRecord(expandedMarcRecord);
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
    public static MarcRecord expandMarcRecord(Map<String, MarcRecord> records, String recordId, boolean keepAutFields) throws RawRepoException {
        final Stopwatch stopWatch = new Stopwatch();
        MarcRecord commonRecord = null;
        final Map<String, MarcRecord> authorityRecords = new HashMap<>();

        // Key is the recordId and value is the record. AgencyId have to be found in the record
        for (Map.Entry<String, MarcRecord> entry : records.entrySet()) {
            final MarcRecordReader reader = new MarcRecordReader(entry.getValue());
            final String foundRecordId = reader.getRecordId();
            final String foundAgencyId = reader.getAgencyId();
            logger.debug("Found record in expand collection: {}:{}", foundRecordId, foundAgencyId);
            if (recordId.equals(foundRecordId)) {
                commonRecord = new MarcRecord(entry.getValue());
            } else if ("870979".equals(foundAgencyId)) {
                authorityRecords.put(foundRecordId, new MarcRecord(entry.getValue()));
            }
        }

        logger.debug("Stopwatch - {} took {} ms", "expandMarcRecord", stopWatch.getElapsedTime(TimeUnit.MILLISECONDS));

        if (commonRecord == null) {
            throw new RawRepoException("The record collection doesn't contain a common record");
        }

        return doExpand(commonRecord, authorityRecords, keepAutFields);
    }

    /**
     * The function takes a set of  records and return a common marc record expanded with authority fields (if any)
     *
     * @param records  The collection of records
     * @param recordId The id of the record to expand
     * @return a single common record expanded with authority data
     * @throws RawRepoException if the collection doesn't contain the necessary records
     */
    public static MarcRecord expandMarcRecord(Map<String, MarcRecord> records, String recordId) throws RawRepoException {
        return expandMarcRecord(records, recordId, false);
    }

    private static MarcRecord doExpand(MarcRecord commonRecord, Map<String, MarcRecord> authorityRecords, boolean keepAutFields) throws RawRepoException {
        final MarcRecord expandedRecord = new MarcRecord();
        /*
         * Okay, here are (some) of the rules for expanding with auth records:
         * Fields that can contain AUT are: 100, 110, 600, 610, 700, 710, 770, 780, 845 or 846
         * AUT reference are located in *5 and *6
         *
         * A field points to AUT data if:
         * Field name is either 100, 110, 600, 610, 700, 710, 770, 780, 845 or 846
         * And contains subfields *5 and *6
         *
         * Rules for expanding are:
         * Remove *5 and *6 if keepAutFields are false
         * Add all subfields from AUT record field 100 or 110 at the same location as *5
         * For fields 100, 600, 700, 770:
         * If AUT record contains field 400 or 500 then add that field as well to the expanded record but as field 900
         * For fields 110, 610, 710, 780:
         * If AUT record contains field 410 or 510 then add that field as well to the expanded record but as field 910
         */

        // Record doesn't have any authority record references, so just return the same record
        if (!hasAutFields(commonRecord)) {
            return commonRecord;
        }

        final MarcRecordReader reader = new MarcRecordReader(commonRecord);
        final int authNumerator = findMaxAuthNumerator(commonRecord.getFields());
        handleRepeatableField(reader.getFieldAll("100"), expandedRecord, authorityRecords, keepAutFields, authNumerator);
        handleRepeatableField(reader.getFieldAll("110"), expandedRecord, authorityRecords, keepAutFields, authNumerator);
        handleRepeatableField(reader.getFieldAll("600"), expandedRecord, authorityRecords, keepAutFields, authNumerator);
        handleRepeatableField(reader.getFieldAll("610"), expandedRecord, authorityRecords, keepAutFields, authNumerator);
        handleRepeatableField(reader.getFieldAll("700"), expandedRecord, authorityRecords, keepAutFields, authNumerator);
        handleRepeatableField(reader.getFieldAll("710"), expandedRecord, authorityRecords, keepAutFields, authNumerator);
        handleRepeatableField(reader.getFieldAll("770"), expandedRecord, authorityRecords, keepAutFields, authNumerator);
        handleRepeatableField(reader.getFieldAll("780"), expandedRecord, authorityRecords, keepAutFields, authNumerator);
        handleRepeatableField(reader.getFieldAll("845"), expandedRecord, authorityRecords, keepAutFields, authNumerator);
        handleRepeatableField(reader.getFieldAll("846"), expandedRecord, authorityRecords, keepAutFields, authNumerator);

        for (MarcField field : commonRecord.getFields()) {
            if (!AUTHORITY_FIELD_LIST.contains(field.getName())) {
                expandedRecord.getFields().add(new MarcField(field));
            }
        }

        sortFields(expandedRecord);

        return expandedRecord;
    }

    private static int findMaxAuthNumerator(List<MarcField> fields) {
        int authNumerator = 1001;
        for (MarcField field : fields) {
            final MarcFieldReader fieldReader = new MarcFieldReader(field);
            if (fieldReader.hasSubfield("å")) {
                try {
                    final int numerator = Integer.parseInt(fieldReader.getValue("å"));
                    if (numerator > authNumerator) {
                        authNumerator = numerator + 1;
                    }
                } catch (NumberFormatException ex) {
                    final String message = String.format("Ugyldig værdi i delfelt %s *å. Forventede et tal men fik '%s' - ignorerer", field.getName(), fieldReader.getValue("å"));
                    logger.debug(message);
                }
            }
        }

        return authNumerator;
    }

    private static void handleRepeatableField(List<MarcField> fields, MarcRecord expandedRecord, Map<String, MarcRecord> authorityRecords, boolean keepAutFields, int authNumerator) throws RawRepoException {
        for (MarcField field : fields) {
            final MarcFieldReader fieldReader = new MarcFieldReader(field);

            if (fieldReader.hasSubfield("5") && fieldReader.hasSubfield("6")) {
                final String authRecordId = fieldReader.getValue("6");
                final MarcRecord authRecord = authorityRecords.get(authRecordId);

                if (authRecord == null) {
                    final String message = String.format("Autoritetsposten '%s' blev ikke fundet i forbindelse med ekspandering af fællesskabsposten", authRecordId);
                    logger.error(message);
                    throw new RawRepoException(message);
                }

                final MarcField expandedField = new MarcField(field);
                final MarcRecordReader authRecordReader = new MarcRecordReader(authRecord);
                String authAuthorFieldName = "";

                int mode = 0;
                switch (field.getName()) {
                    case "100":
                        mode = 1;
                        authAuthorFieldName = "100";
                        break;
                    case "600":
                    case "700":
                    case "770":
                        mode = 2;
                        authAuthorFieldName = "100";
                        break;
                    case "110":
                        mode = 1;
                        authAuthorFieldName = "110";
                        break;
                    case "610":
                    case "710":
                    case "780":
                        mode = 2;
                        authAuthorFieldName = "110";
                        break;
                    case "845":
                        mode = 3;
                        authAuthorFieldName = "133";
                        break;
                    case "846":
                        mode = 4;
                        authAuthorFieldName = "134";
                        break;
                }
                if (!authRecordReader.hasField(authAuthorFieldName)) {
                    return;
                }
                final MarcField authAuthorField = new MarcField(authRecordReader.getField(authAuthorFieldName));

                addMainField(expandedField, authAuthorField, keepAutFields);

                String fieldReference = field.getName();
                if (mode == 1 || mode == 2) {
                    // x00 and 770 puts 400 and 500 in 900 fields and x10 and 780 puts 410 and 510 in 910 fields - this is so fun
                    boolean hasAdditionalFields;
                    List<String> mayNeedFourFiveHundred = Arrays.asList("00", "70");
                    if (mayNeedFourFiveHundred.contains(field.getName().substring(1))) {
                        hasAdditionalFields = authRecordReader.hasField("400") || authRecordReader.hasField("500");
                    } else {
                        hasAdditionalFields = authRecordReader.hasField("410") || authRecordReader.hasField("510");
                    }
                    if (mode == 2 && hasAdditionalFields) {
                        // The field is repeatable, so we add a numerator value to the *z content
                        if (!fieldReader.hasSubfield("å")) {
                            expandedField.getSubfields().add(0, new MarcSubField("å", Integer.toString(authNumerator)));
                            fieldReference += "/" + authNumerator;
                            authNumerator++;
                        } else {
                            fieldReference += "/" + fieldReader.getValue("å");
                        }
                    }
                    if (mayNeedFourFiveHundred.contains(field.getName().substring(1))) {
                        if (hasAdditionalFields) {
                            addAdditionalFields("900", expandedRecord, authRecordReader.getFieldAll("400"), authAuthorField, fieldReference);
                            addAdditionalFields("900", expandedRecord, authRecordReader.getFieldAll("500"), authAuthorField, fieldReference);
                        }
                    } else {
                        if (hasAdditionalFields) {
                            addAdditionalFields("910", expandedRecord, authRecordReader.getFieldAll("410"), authAuthorField, fieldReference);
                            addAdditionalFields("910", expandedRecord, authRecordReader.getFieldAll("510"), authAuthorField, fieldReference);
                        }
                    }
                } else {
                    // The universe/series fields is repeatable, so we add a numerator value to the *z content
                    if (!fieldReader.hasSubfield("å")) {
                        expandedField.getSubfields().add(0, new MarcSubField("å", Integer.toString(authNumerator)));
                        fieldReference += "/" + authNumerator;
                        authNumerator++;
                    } else {
                        fieldReference += "/" + fieldReader.getValue("å");
                    }
                    if (mode == 3) {
                        addAdditionalFields("945", expandedRecord, authRecordReader.getFieldAll("433"), authAuthorField, fieldReference);
                    } else {
                        addAdditionalFields("945", expandedRecord, authRecordReader.getFieldAll("434"), authAuthorField, fieldReference);
                    }
                }
                expandedRecord.getFields().add(new MarcField(expandedField));
            } else {
                expandedRecord.getFields().add(new MarcField(field));
            }
        }
    }

    private static void addMainField(MarcField field, MarcField authField, boolean keepAutFields) {
        // Find the index of where the AUT reference subfields are in the field
        // We need to add the AUT content at that location
        int authSubfieldIndex = 0;
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
            final MarcFieldWriter expandedFieldWriter = new MarcFieldWriter(field);
            expandedFieldWriter.removeSubfield("5");
            expandedFieldWriter.removeSubfield("6");
        }
        field.setIndicator("00");
        if ("845".equals(field.getName()) || "846".equals(field.getName())) {
            for (MarcSubField authSubfield : authField.getSubfields()) {
                if ("a".equals(authSubfield.getName())) {
                    field.getSubfields().add(authSubfieldIndex++, new MarcSubField(authSubfield));
                }
            }
        } else {
            for (MarcSubField authSubfield : authField.getSubfields()) {
                field.getSubfields().add(authSubfieldIndex++, new MarcSubField(authSubfield));
            }
        }
    }

    private static void addAdditionalFields(String fieldName, MarcRecord record, List<MarcField> authFields, MarcField authAuthorField, String fieldReference) {
        final boolean universeFields = "945".equals(fieldName); // I don't like this, but for the moment only universe/series put things in 945
        for (MarcField authField : authFields) {
            final MarcField additionalField = new MarcField(fieldName, "00");
            String subfieldwValue = null;
            for (MarcSubField authSubfield : authField.getSubfields()) {
                if ("w".equals(authSubfield.getName())) {
                    if (!universeFields) {
                        subfieldwValue = authSubfield.getValue();
                    }
                } else {
                    if (universeFields) {
                        if ("a".equals(authSubfield.getName())) {
                            // there will at least be a subfield 8 which isn't wanted - only subfield a should be copied
                            additionalField.getSubfields().add(new MarcSubField(authSubfield));
                        }
                    } else {
                        additionalField.getSubfields().add(new MarcSubField(authSubfield));
                    }
                }
            }

            if (!universeFields) {
                if (subfieldwValue != null) {
                    if ("tidligere navn".equals(subfieldwValue)) {
                        additionalField.getSubfields().add(new MarcSubField("x", "se også under det senere navn"));
                    } else if ("senere navn".equals(subfieldwValue)) {
                        additionalField.getSubfields().add(new MarcSubField("x", "se også under det tidligere navn"));
                    } else {
                        additionalField.getSubfields().add(new MarcSubField("x", subfieldwValue));
                    }
                } else {
                    if (Arrays.asList("500", "510").contains(authField.getName())) {
                        additionalField.getSubfields().add(new MarcSubField("x", "se også"));
                    } else {
                        additionalField.getSubfields().add(new MarcSubField("x", "se"));
                    }
                }
            }

            final StringBuilder sb = new StringBuilder();

            /*
             * Generelt om ekspansion af felt 410 til 910:
             * For ekspansion af felt 410 i A-posten (som henvisning til 610, 710 og 780) gælder:
             * Indhold fra felt 110 - alle delfelterne undtagen eijk - skal skrives i B-postens felt 910 *w i den rækkefølge de optræder i A-posten. Efter hvert delfelt skal skrives et punktum.
             * Indhold fra delfelterne e, i, j og k skal skrives i en blød parentes med : mellem. Der skal være blanktegn på begge sider af semikolon. Se eksempler.
             * Kommer et delfelt c efter et af delfelterne e, i, j eller k skal der være et punktum efter den bløde parentes afsluttes. Se eksempel.
             */
            if (!universeFields) {
                if ("110".equals(authAuthorField.getName())) {
                    final List<String> parenthesesSubFieldNames = Arrays.asList("e", "i", "j", "k");

                    String previousSubFieldName = "";

                    for (MarcSubField subField : authAuthorField.getSubfields()) {
                        if (parenthesesSubFieldNames.contains(subField.getName())) {
                            if (parenthesesSubFieldNames.contains(previousSubFieldName)) {
                                // Continue parentheses
                                sb.append(" : ");
                                sb.append(subField.getValue());
                            } else {
                                // New parentheses
                                sb.append(" (");
                                sb.append(subField.getValue());
                            }
                        } else {
                            if (parenthesesSubFieldNames.contains(previousSubFieldName)) {
                                // End parentheses
                                sb.append("). ");
                                sb.append(subField.getValue());
                            } else {
                                if (!"".equals(previousSubFieldName)) {
                                    sb.append(". ");
                                }
                                sb.append(subField.getValue());
                            }
                        }
                        previousSubFieldName = subField.getName();
                    }
                    if (parenthesesSubFieldNames.contains(previousSubFieldName)) {
                        sb.append(")");
                    }
                } else {
                    final MarcFieldReader authField100Reader = new MarcFieldReader(authAuthorField);
                    final boolean hasAuthField100A = authField100Reader.hasSubfield("a");
                    final boolean hasAuthField100H = authField100Reader.hasSubfield("h");

                    if (hasAuthField100A && hasAuthField100H) {
                        sb.append(authField100Reader.getValue("a"));
                        sb.append(", ");
                        sb.append(authField100Reader.getValue("h"));
                    } else if (hasAuthField100A) {
                        sb.append(authField100Reader.getValue("a"));
                    } else if (hasAuthField100H) {
                        sb.append(authField100Reader.getValue("h"));
                    }

                    if (authField100Reader.hasSubfield("c")) {
                        sb.append(" (");
                        sb.append(authField100Reader.getValue("c"));
                        sb.append(")");
                    }
                }
                additionalField.getSubfields().add(new MarcSubField("w", sb.toString()));

            }
            additionalField.getSubfields().add(new MarcSubField("z", fieldReference));
            record.getFields().add(additionalField);
        }
    }

    private static void sortFields(MarcRecord record) {
        // First sort by field name then sort by subfield å
        record.getFields().sort((m1, m2) -> {
            if (m1.getName().equals(m2.getName())) {
                return getAaIntegerValue(m1) - getAaIntegerValue(m2);
            }

            return m1.getName().compareTo(m2.getName());
        });
    }

    private static int getAaIntegerValue(MarcField marcField) {
        final MarcFieldReader marcFieldReader = new MarcFieldReader(marcField);

        if (marcFieldReader.hasSubfield("å")) {
            try {
                return Integer.parseInt(marcFieldReader.getValue("å"));
            } catch (NumberFormatException e) {
                logger.error("Got invalid integer value in *å: {}", marcFieldReader.getValue("å"));
                return 0;
            }
        } else {
            return 0;
        }
    }

    private static boolean hasAutFields(MarcRecord record) {
        for (MarcField field : record.getFields()) {
            final MarcFieldReader fieldReader = new MarcFieldReader(field);
            if (fieldReader.hasSubfield("5") && fieldReader.hasSubfield("6")) {
                return true;
            }
        }

        return false;
    }

}
