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
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import java.util.*;

/**
 *
 * @author DBC {@literal <dbc.dk>}
 */
public class ValidateRelations {

    private static final XLogger logger = XLoggerFactory.getXLogger(ValidateRelations.class);

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
        tmp.put(MarcXChangeMimeType.ARTICLE, makeValidatorArticle());
        tmp.put(MarcXChangeMimeType.AUTHORITY, makeValidatorAuthority());
        tmp.put(MarcXChangeMimeType.ENRICHMENT, makeValidatorEnrichment());
        tmp.put(MarcXChangeMimeType.LITANALYSIS, makeValidatorLitAnalysis());
        tmp.put(MarcXChangeMimeType.MATVURD, makeValidatorMatVurd());
        return tmp;
    }

    private static Validator makeValidatorMarcXchange() {
        return new Validator() {
            @Override
            public void validate(RawRepoDAO dao, RecordId recordId, Set<RecordId> refers) throws RawRepoException {
                ArrayList<String> parentMimeTypes = new ArrayList<>();
                for (RecordId refer : refers) {
                    if (recordId.getBibliographicRecordId().equals(refer.getBibliographicRecordId())) {
                        logger.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, too many sibling relations for MARCXCHANGE (text/(decentral+)marcxchange > 0)");
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
                        logger.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, too many parent relations: " + marcxCount);
                    }

                    // A record can have multiple AUTHORITY parents so we have to use removeAll() as remove() only takes care of the first occurrence
                    parentMimeTypes.removeAll(Collections.singleton(MarcXChangeMimeType.AUTHORITY));
                    if (parentMimeTypes.size() > 0) {
                        logger.error("Validate constraint: " + recordId + " -> " + refers);
                        logger.error("Mimetypes: " + parentMimeTypes.toString());
                        throw new RawRepoException("Error setting relations, parent relation of invalid mimetype - VMX");
                    }
                }
            }
        };
    }

    private static Validator makeValidatorArticle() {
        return new Validator() {
            @Override
            public void validate(RawRepoDAO dao, RecordId recordId, Set<RecordId> refers) throws RawRepoException {
                ArrayList<String> parentMimeTypes = new ArrayList<>();
                for (RecordId refer : refers) {
                    if (recordId.getBibliographicRecordId().equals(refer.getBibliographicRecordId())) {
                        logger.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, too many sibling relations for ARTICLE (text/(decentral+)marcxchange > 0)");
                    } else {
                        String mimeType = dao.getMimeTypeOf(refer.getBibliographicRecordId(), refer.getAgencyId());
                        parentMimeTypes.add(mimeType);
                    }
                }
                if (parentMimeTypes.size() > 0) {
                    int marcxCount = parentMimeTypes.size();
                    parentMimeTypes.remove(MarcXChangeMimeType.MARCXCHANGE);
                    parentMimeTypes.remove(MarcXChangeMimeType.ARTICLE);
                    parentMimeTypes.removeAll(Collections.singleton(MarcXChangeMimeType.AUTHORITY));
                    marcxCount -= parentMimeTypes.size();
                    if (parentMimeTypes.size() > 0) {
                        logger.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, too many parent relations: " + marcxCount);
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
                    logger.error("Validate constraint: " + recordId + " -> " + refers);
                    throw new RawRepoException("authority records cannot have outbound relations");
                }
            }
        };
    }

    private static Validator makeValidatorLitAnalysis() {
        return new Validator() {
            @Override
            public void validate(RawRepoDAO dao, RecordId recordId, Set<RecordId> refers) throws RawRepoException {
                ArrayList<String> parentMimeTypes = new ArrayList<>();
                for (RecordId refer : refers) {
                    if (recordId.getBibliographicRecordId().equals(refer.getBibliographicRecordId())) {
                        logger.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, too many sibling relations for LITANALYSIS (text/(decentral+)marcxchange > 0)");
                    } else {
                        String mimeType = dao.getMimeTypeOf(refer.getBibliographicRecordId(), refer.getAgencyId());
                        parentMimeTypes.add(mimeType);
                    }
                }
                if (parentMimeTypes.size() > 0) {
                    int marcxCount = parentMimeTypes.size();
                    parentMimeTypes.remove(MarcXChangeMimeType.MARCXCHANGE);
                    parentMimeTypes.remove(MarcXChangeMimeType.ARTICLE);
                    parentMimeTypes.removeAll(Collections.singleton(MarcXChangeMimeType.AUTHORITY));
                    marcxCount -= parentMimeTypes.size();
                    if (parentMimeTypes.size() > 0) {
                        logger.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, too many parent relations: " + marcxCount);
                    }
                }
            }
        };
    }

    /**
     * Validates the relations from a material evaluation record (MATVURD)
     * A MATVURD record is allowed to point to zero or more marcxchange records only
     *
     * @return Validator which validates the relations for a MATVURD record.
     */
    private static Validator makeValidatorMatVurd() {
        return new Validator() {
            @Override
            public void validate(RawRepoDAO dao, RecordId recordId, Set<RecordId> refers) throws RawRepoException {
                ArrayList<String> parentMimeTypes = new ArrayList<>();
                for (RecordId refer : refers) {
                    if (recordId.getBibliographicRecordId().equals(refer.getBibliographicRecordId())) {
                        logger.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, too many sibling relations for MATVURD (text/(decentral+)marcxchange > 0)");
                    } else {
                        String mimeType = dao.getMimeTypeOf(refer.getBibliographicRecordId(), refer.getAgencyId());
                        parentMimeTypes.add(mimeType);
                    }
                }
                if (parentMimeTypes.size() > 0) {
                    int marcxCount = parentMimeTypes.size();
                    parentMimeTypes.removeAll(Collections.singleton(MarcXChangeMimeType.MARCXCHANGE));
                    marcxCount -= parentMimeTypes.size();
                    if (parentMimeTypes.size() > 0) {
                        logger.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, too many parent relations: " + marcxCount);
                    }
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
                    logger.error("Validate constraint: " + recordId + " -> " + refers);
                    throw new RawRepoException("Error setting relations, too many sibling relations (text/enrichment+marcxchange > 1)");
                } else if (siblingMimeTypes.size() > 0) {
                    siblingMimeTypes.remove(MarcXChangeMimeType.ENRICHMENT);
                    siblingMimeTypes.remove(MarcXChangeMimeType.MARCXCHANGE);
                    siblingMimeTypes.remove(MarcXChangeMimeType.ARTICLE);
                    siblingMimeTypes.remove(MarcXChangeMimeType.AUTHORITY);
                    siblingMimeTypes.remove(MarcXChangeMimeType.LITANALYSIS);
                    siblingMimeTypes.remove(MarcXChangeMimeType.MATVURD);
                    if (!siblingMimeTypes.isEmpty()) {
                        logger.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, sibling relation of invalid mimetype");
                    }
                    parentMimeTypes.remove(MarcXChangeMimeType.AUTHORITY);
                    if (!parentMimeTypes.isEmpty()) {
                        logger.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, parent relation of invalid mimetype - VE1");
                    }
                } else if (parentMimeTypes.size() > 0) {
                    int marcxCount = parentMimeTypes.size();
                    parentMimeTypes.remove(MarcXChangeMimeType.MARCXCHANGE);
                    marcxCount -= parentMimeTypes.size();
                    if (marcxCount > 1) {
                        logger.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, too many parent relations");
                    }
                    parentMimeTypes.remove(MarcXChangeMimeType.AUTHORITY);
                    if (parentMimeTypes.size() > 0) {
                        logger.error("Validate constraint: " + recordId + " -> " + refers);
                        throw new RawRepoException("Error setting relations, parent relation of invalid mimetype - VE2");
                    }
                }
            }
        };
    }
}
