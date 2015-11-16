/*
 * Copyright (C) 2014 DBC A/S (http://dbc.dk/)
 *
 * This is part of dbc-rawrepo
 *
 * dbc-rawrepo is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * dbc-rawrepo is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package dk.dbc.marcxmerge;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

/**
 *
 * @author Morten Bøgeskov <mb@dbc.dk>
 */
public class FieldRules {

    public static final String INVALID_DEFAULT = "";
    public static final String OVERWRITE_DEFAULT = "001;004;005;013;014;017;035;036;240;243;247;300"
                                                   + ";008 009 038 039 100 110 239 245 652 654"; // Opstillingsdata
    public static final String IMMUTABLE_DEFAULT = "010;020;990;991;996";
    public static final String VALID_REGEX_DANMARC2 = "\\d{3}";

    private final Pattern validRegex;
    private final Set<String> invalid;
    private final Set<String> immutable;
    private final Set<String> remove;
    private final MarcXFixup[] fixups;

    private final Map<String, Set<String>> overwriteCollections;

    private static Set<String> collectionInit(String init) {
        HashSet set = new HashSet();
        Collections.addAll(set, init.split(";"));
        return set;
    }

    private static Map<String, Set<String>> overwriteCollectionsInit(String init) {
        Map<String, Set<String>> map = new HashMap<>();
        String[] groups = init.split(";");
        for (String group : groups) {
            HashSet set = new HashSet();
            String[] tags = group.split(" ");
            for (String tag : tags) {
                set.add(tag);
                if (map.containsKey(tag)) {
                    throw new IllegalArgumentException("Error initializing overwriteCollections, field: " + tag + " is repeated");
                }
                map.put(tag, set);
            }
        }
        return map;
    }

    /**
     * class for ruleset for an individual marcx merge
     *
     * needs to have called
     * {@link #registerLocalField(java.lang.String) registerLocalField} for
     * every field to know if a
     * {@link #removeField(java.lang.String) removeField} should retruen true or
     * false
     */
    public class RuleSet {

        private final Set<String> immutable = new HashSet<>();
        private final Set<String> remove = new HashSet<>();

        private RuleSet(Set<String> immutable, Set<String> remove) {
            this.immutable.addAll(immutable);
            this.remove.addAll(remove);
        }

        /**
         * Register the presence of a local field, and all the fields in it's
         * collection
         *
         * @param field
         */
        public void registerLocalField(String field) {
            if (overwriteCollections.containsKey(field)) {
                remove.addAll(overwriteCollections.get(field));
            }
        }

        /**
         * The presence of this field is not wanted
         *
         * @param field
         * @param includeAllFields Do not restrict to validRegex
         * @return boolean
         */
        public boolean invalidField(String field, boolean includeAllFields) {
            return !includeAllFields && !validRegex.matcher(field).matches() || invalid.contains(field);
        }

        /**
         * Remove this field from the common data
         *
         * @param field
         * @return boolean
         */
        public boolean removeField(String field) {
            return remove.contains(field);
        }

        /**
         * This field cannot be overwritten by a local value
         *
         * @param field
         * @return boolean
         */
        public boolean immutableField(String field) {
            return immutable.contains(field);
        }
    }

    /**
     * Default setup
     */
    public FieldRules() {
        this.invalid = collectionInit(INVALID_DEFAULT);
        this.immutable = collectionInit(IMMUTABLE_DEFAULT);
        this.remove = new HashSet<>();
        this.overwriteCollections = overwriteCollectionsInit(OVERWRITE_DEFAULT);
        this.validRegex = Pattern.compile(VALID_REGEX_DANMARC2, Pattern.MULTILINE);
        this.fixups = DEFAULT_FIXUPS;
    }

    /**
     *
     * @param immutable  fields that can't be modified
     * @param overwrite  fields that are replacing (groups (of tags separated by
     *                   space) separated by ;)
     * @param invalid    fields that should always be removed
     * @param validRegex regex that tag must match to be considered valid
     */
    public FieldRules(String immutable, String overwrite, String invalid, String validRegex) {
        this.invalid = collectionInit(invalid);
        this.immutable = collectionInit(immutable);
        this.remove = new HashSet<>();
        this.overwriteCollections = overwriteCollectionsInit(overwrite);
        this.validRegex = Pattern.compile(validRegex, Pattern.MULTILINE);
        this.fixups = DEFAULT_FIXUPS;
    }

    /**
     *
     * @param immutable  fields that can't be modified
     * @param overwrite  fields that are replacing (groups (of tags separated by
     *                   space) separated by ;)
     * @param invalid    fields that should always be removed
     * @param validRegex regex that tag must match to be considered valid
     * @param fixups     list of fixes that needs to be run post merge
     */
    public FieldRules(String immutable, String overwrite, String invalid, String validRegex, final MarcXFixup[] fixups) {
        this.invalid = collectionInit(invalid);
        this.immutable = collectionInit(immutable);
        this.remove = new HashSet<>();
        this.overwriteCollections = overwriteCollectionsInit(overwrite);
        this.validRegex = Pattern.compile(validRegex, Pattern.MULTILINE);
        this.fixups = fixups.clone();
    }

    /**
     *
     * @return
     */
    public RuleSet newRuleSet() {
        return new RuleSet(immutable, remove);
    }

    /**
     *
     * @return
     */
    public List<MarcXFixup> getFixups() {
        return Arrays.asList(fixups);
    }

    static Element findFirstSubField(Document doc, String field, String subField) {
        Element root = doc.getDocumentElement();
        for (Node datafield = root.getFirstChild() ; datafield != null ; datafield = datafield.getNextSibling()) {
            if (datafield.getNodeType() == Node.ELEMENT_NODE
                && "datafield".equals(datafield.getLocalName())
                && field.equals(( (Element) datafield ).getAttribute("tag"))) {
                for (Node subfield = datafield.getFirstChild() ; subfield != null ; subfield = subfield.getNextSibling()) {
                    if (subfield.getNodeType() == Node.ELEMENT_NODE
                        && "subfield".equals(subfield.getLocalName())
                        && subField.equals(( (Element) subfield ).getAttribute("code"))) {
                        return (Element) subfield;
                    }
                }
            }
        }
        return null;
    }

    public interface MarcXFixup {

        /**
         *
         * @param target
         * @param common
         * @param enrich
         */
        void fix(Document target, Document common, Document enrich);
    }

    private static final MarcXFixup DEFAULT_FIXUPS[] = {
        // danMARC2: 001 *c
        new MarcXFixup() {
            @Override
            public void fix(Document target, Document common, Document enrich) {
                if ("danMARC2".equals(target.getDocumentElement().getAttribute("format"))) {
                    Element target001cNode = findFirstSubField(target, "001", "c");
                    Element common001cNode = findFirstSubField(common, "001", "c");
                    Element enrich001cNode = findFirstSubField(enrich, "001", "c");
                    if (target001cNode != null && common001cNode != null && enrich001cNode != null) {
                        String common001c = common001cNode.getTextContent();
                        String enrich001c = enrich001cNode.getTextContent();
                        if (Long.parseLong(common001c, 10) > Long.parseLong(enrich001c, 10)) {
                            target001cNode.setTextContent(common001c);
                        } else {
                            target001cNode.setTextContent(enrich001c);
                        }
                    }
                }
            }
        },

    };
}
