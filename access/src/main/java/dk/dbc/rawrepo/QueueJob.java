/*
 * dbc-rawrepo-access
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043
 *
 * This file is part of dbc-rawrepo-access.
 *
 * dbc-rawrepo-access is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo-access is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with dbc-rawrepo-access.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.rawrepo;

import dk.dbc.jsonextractor.JsonValue;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.function.Function;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.TextMessage;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class QueueJob implements Serializable {

    private static final String PROP_NAME = "objectVersion";
    private static final String PROP_VALUE = "1.1";
    public static final String MESSAGE_SELECTOR = PROP_NAME + " = '" + PROP_VALUE + "'";

    private static final long serialVersionUID = -2044611690216364263L;

    RecordId recordId;
    String error;

    QueueJob() {
    }

    private QueueJob(String bibliographicRecordId, int agencyId, String error) {
        this.recordId = new RecordId(bibliographicRecordId, agencyId);
        this.error = error;
    }

    public QueueJob(String bibliographicRecordId, int agencyId) {
        this.recordId = new RecordId(bibliographicRecordId, agencyId);
        this.error = null;
    }

    public QueueJob(RecordId recordId) {
        this.recordId = recordId;
        this.error = null;
    }

    public RecordId getJob() {
        return recordId;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public TextMessage jmsMessage(JMSContext context) throws JMSException {
        TextMessage message = context.createTextMessage(toJSON());
        message.setStringProperty(QueueJob.PROP_NAME, QueueJob.PROP_VALUE);
        message.setStringProperty("bibliographicRecordId", recordId.getBibliographicRecordId());
        message.setStringProperty("agencyId", String.valueOf(recordId.getAgencyId()));
        return message;
    }

    public static QueueJob fromMessage(Message message) throws JMSException {
        if (message instanceof TextMessage) {
            TextMessage textMessage = (TextMessage) message;
            String json = textMessage.getText();
            return fromJSON(json);
        } else {
            throw new JMSException("Not of type TextMessage");
        }

    }

    public static QueueJob fromJSON(String json) {
        return JSON_MAPPER.apply(json);
    }

    private static final Function<String, QueueJob> JSON_MAPPER = new Function<String, QueueJob>() {

        JsonValue<String> bibliographicRecordId = JsonValue.stringSetter("bibliographicRecordId");
        JsonValue<Long> agencyId = JsonValue.longSetter("agencyId");
        JsonValue<String> error = JsonValue.stringOrNullSetter("error");

        JsonValue.Extractor extractor = JsonValue.extractor()
                .at("job", "bibliographicRecordId").set(bibliographicRecordId)
                .at("job", "agencyId").set(agencyId)
                .at("error").set(error);

        @Override
        public QueueJob apply(String json) {
            try {
                synchronized (this) {
                    bibliographicRecordId.reset();
                    agencyId.reset();
                    error.reset();

                    extractor.process(json);

                    return new QueueJob(bibliographicRecordId.orThrow(),
                                        (int) (long) agencyId.orThrow(),
                                        error.orElse(null));
                }
            } catch (IOException ex) {
                throw new RuntimeException("Error processing JSON to QueueJob", ex);
            }
        }
    };

    @Override
    public String toString() {
        return "QueueJob{" + "job=" + recordId + ", error=" + error + '}';
    }

    public String toJSON() {
        StringBuilder output = new StringBuilder();
        if (error == null) {
            output.append("{\"error\":null,\"job\":{\"bibliographicRecordId\":\"");
        } else {
            output.append("{\"error\":\"");
            jsonStringContent(error, output);
            output.append("\",\"job\":{\"bibliographicRecordId\":\"");
        }
        jsonStringContent(recordId.getBibliographicRecordId(), output);
        output.append("\",\"agencyId\":").append(recordId.getAgencyId()).append("}}");
        return output.toString();
    }

    private static final HashMap<Character, String> JSON_CODES = makeJsonCodes();
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private static HashMap<Character, String> makeJsonCodes() {
        HashMap<Character, String> map = new HashMap<>();
        map.put('\\', "\\\\");
        map.put('\"', "\\\"");
        map.put('\b', "\\b");
        map.put('\f', "\\f");
        map.put('\n', "\\n");
        map.put('\r', "\\r");
        map.put('\t', "\\t");
        map.put('/', "\\/"); // JSON rule, that doesn't make much sense
        return map;
    }

    private static void jsonStringContent(String src, StringBuilder sb) {
        for (char c : src.toCharArray()) {
            String str = JSON_CODES.get(c);
            if (str != null) {
                sb.append(str);
            } else if (c >= '\u0000' && c <= '\u001F' ||
                       c >= '\u007F' && c <= '\u009F' ||
                       c >= '\u2000' && c <= '\u20FF') {
                sb.append("\\u")
                        .append(HEX[( c >> 12 ) & 15])
                        .append(HEX[( c >> 8 ) & 15])
                        .append(HEX[( c >> 4 ) & 15])
                        .append(HEX[c & 15]);
            } else {
                sb.append(c);
            }
        }
    }

}
