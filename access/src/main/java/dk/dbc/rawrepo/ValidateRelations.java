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

import dk.dbc.marcxmerge.MarcXChangeMimeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class ValidateRelations {

    private static final Logger log = LoggerFactory.getLogger(ValidateRelations.class);

    public static void validate(RawRepoDAO dao, RecordId recordId, Set<RecordId> refers) throws RawRepoException {
        String mimeType = dao.getMimeTypeOf(recordId.getBibliographicRecordId(), recordId.getAgencyId());
        if (validators.containsKey(mimeType)) {
            validators.get(mimeType).validate(dao, recordId, refers);
        }
    }

    private interface Validator {

        void validate(RawRepoDAO dao, RecordId recordId, Set<RecordId> refers) throws RawRepoException;
    }

    private static final Map<String, Validator> validators = initializeValidators();

    private static Map<String, Validator> initializeValidators() {
        HashMap<String, Validator> tmp = new HashMap<>();
        tmp.put(MarcXChangeMimeType.MARCXCHANGE, makeValidatorMarcXchange());
        tmp.put(MarcXChangeMimeType.AUTHORITY, makeValidatorAuthority());
        tmp.put(MarcXChangeMimeType.ENRICHMENT, makeValidatorEnrichment());
        return tmp;
    }

    private static Validator makeValidatorMarcXchange() {
        return new Validator() {
            @Override
            public void validate(RawRepoDAO dao, RecordId recordId, Set<RecordId> refers) throws RawRepoException {
                ArrayList<String> parentMimeTypes = new ArrayList<>();
                for (RecordId refer : refers) {
                    if (recordId.getBibliographicRecordId().equals(refer.getBibliographicRecordId())) {
                        log.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, too many sibling relations (text/(decentral+)marcxchange > 0)");
                    } else {
                        String mimeType = dao.getMimeTypeOf(refer.getBibliographicRecordId(), refer.getAgencyId());
                        parentMimeTypes.add(mimeType);
                    }
                }
                if (parentMimeTypes.size() > 0) {
                    int marcxCount = parentMimeTypes.size();
                    parentMimeTypes.remove(MarcXChangeMimeType.MARCXCHANGE);
                    marcxCount -= parentMimeTypes.size();
                    if (marcxCount > 1) {
                        log.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, too many parent relations: " + marcxCount);
                    }
                    parentMimeTypes.remove(MarcXChangeMimeType.AUTHORITY);
                    if (parentMimeTypes.size() > 0) {
                        log.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, parent relation of invalid mimetype - VMX");
                    }
                }
            }
        };
    }

    private static Validator makeValidatorAuthority() {
        return new Validator() {
            @Override
            public void validate(RawRepoDAO dao, RecordId recordId, Set<RecordId> refers) throws RawRepoException {
                if (!refers.isEmpty()) {
                    log.error("Validate constraint: " + recordId + " -> " + refers);
                    throw new RawRepoException("authority records cannot have outbound relations");
                }
            }
        };
    }

    private static Validator makeValidatorEnrichment() {
        return new Validator() {
            @Override
            public void validate(RawRepoDAO dao, RecordId recordId, Set<RecordId> refers) throws RawRepoException {
                ArrayList<String> parentMimeTypes = new ArrayList<>();
                ArrayList<String> siblingMimeTypes = new ArrayList<>();
                for (RecordId refer : refers) {
                    String mimeType = dao.getMimeTypeOf(refer.getBibliographicRecordId(), refer.getAgencyId());
                    if (recordId.getBibliographicRecordId().equals(refer.getBibliographicRecordId())) {
                        siblingMimeTypes.add(mimeType);
                    } else {
                        parentMimeTypes.add(mimeType);
                    }
                }
                if (siblingMimeTypes.size() > 1) {
                    log.error("Validate constraint: " + recordId + " -> " + refers);
                    throw new RawRepoException("Error setting relations, too many sibling relations (text/enrichment+marcxchange > 1)");
                } else if (siblingMimeTypes.size() > 0) {
                    siblingMimeTypes.remove(MarcXChangeMimeType.ENRICHMENT);
                    siblingMimeTypes.remove(MarcXChangeMimeType.MARCXCHANGE);
                    siblingMimeTypes.remove(MarcXChangeMimeType.ARTICLE);
                    if (!siblingMimeTypes.isEmpty()) {
                        log.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, sibling relation of invalid mimetype");
                    }
                    parentMimeTypes.remove(MarcXChangeMimeType.AUTHORITY);
                    if (!parentMimeTypes.isEmpty()) {
                        log.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, parent relation of invalid mimetype - VE1");
                    }
                } else if (parentMimeTypes.size() > 0) {
                    int marcxCount = parentMimeTypes.size();
                    parentMimeTypes.remove(MarcXChangeMimeType.MARCXCHANGE);
                    marcxCount -= parentMimeTypes.size();
                    if (marcxCount > 1) {
                        log.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, too many parent relations");
                    }
                    parentMimeTypes.remove(MarcXChangeMimeType.AUTHORITY);
                    if (parentMimeTypes.size() > 0) {
                        log.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, parent relation of invalid mimetype - VE2");
                    }
                }
            }
        };
    }
}
