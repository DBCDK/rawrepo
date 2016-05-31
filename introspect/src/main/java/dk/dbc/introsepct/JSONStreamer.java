/*
 * dbc-rawrepo-introspect
 * Copyright (C) 2015 Dansk Bibliotekscenter a/s, Tempovej 7-11, DK-2750 Ballerup,
 * Denmark. CVR: 15149043*
 *
 * This file is part of dbc-rawrepo-introspect.
 *
 * dbc-rawrepo-introspect is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * dbc-rawrepo-introspect is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with dbc-rawrepo-introspect.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.introsepct;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Collection;
import java.util.Map;
import javax.ejb.Stateless;
import javax.json.Json;
import javax.json.stream.JsonGenerator;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

/**
 *
 * @author DBC <dbc.dk>
 */
@Stateless
public class JSONStreamer {

    private class StreamingOutputImpl implements StreamingOutput {

        Object data;

        public StreamingOutputImpl(Object data) {
            this.data = data;
        }

        @Override
        public void write(OutputStream output) throws IOException, WebApplicationException {
            stream(output, data);
        }
    }

    public StreamingOutput stream(Object data) {
        return new StreamingOutputImpl(data);
    }

    private void stream(OutputStream stream, Object data) throws IOException {

        if (data == null || data instanceof Boolean) {
            stream.write(String.valueOf(data).getBytes("UTF-8"));
            stream.flush();
        } else {
            JsonGenerator generator = Json.createGenerator(stream);
            if (data instanceof Map) {
                Map<String, Object> val = (Map<String, Object>) data;
                generator.writeStartObject();
                writeMap(generator, val);
            } else if (data instanceof Collection) {
                Collection<Object> val = (Collection<Object>) data;
                generator.writeStartArray();
                writeArray(generator, val);
            } else {
                throw new IllegalStateException("Response can only be of object/array");
            }
            generator.writeEnd();
            generator.flush();
        }
    }

    private void writeArray(JsonGenerator generator, Collection<Object> array) {
        for (Object data : array) {
            if (data == null) {
                generator.writeNull();
            } else if (data instanceof Map) {
                Map<String, Object> val = (Map<String, Object>) data;
                generator.writeStartObject();
                writeMap(generator, val);
                generator.writeEnd();
            } else if (data instanceof Collection) {
                Collection<Object> val = (Collection<Object>) data;
                generator.writeStartObject();
                writeArray(generator, val);
                generator.writeEnd();
            } else if (data instanceof Boolean) {
                Boolean val = (Boolean) data;
                generator.write(val);
            } else if (data instanceof Long) {
                Long val = (Long) data;
                generator.write(val);
            } else if (data instanceof Integer) {
                Integer val = (Integer) data;
                generator.write(val);
            } else if (data instanceof Double) {
                Double val = (Double) data;
                generator.write(val);
            } else if (data instanceof Float) {
                Float val = (Float) data;
                generator.write(val);
            } else {
                String val = data.toString();
                generator.write(val);
            }
        }
    }

    private void writeMap(JsonGenerator generator, Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey();
            Object data = entry.getValue();
            if (data == null) {
                generator.writeNull(key);
            } else if (data instanceof Map) {
                Map<String, Object> val = (Map<String, Object>) data;
                generator.writeStartObject(key);
                writeMap(generator, val);
                generator.writeEnd();
            } else if (data instanceof Collection) {
                Collection<Object> val = (Collection<Object>) data;
                generator.writeStartArray(key);
                writeArray(generator, val);
                generator.writeEnd();
            } else if (data instanceof Boolean) {
                Boolean val = (Boolean) data;
                generator.write(key, val);
            } else if (data instanceof Long) {
                Long val = (Long) data;
                generator.write(key, val);
            } else if (data instanceof Integer) {
                Integer val = (Integer) data;
                generator.write(key, val);
            } else if (data instanceof Double) {
                Double val = (Double) data;
                generator.write(key, val);
            } else if (data instanceof Float) {
                Float val = (Float) data;
                generator.write(key, val);
            } else {
                String val = data.toString();
                generator.write(key, val);
            }
        }
    }

}
