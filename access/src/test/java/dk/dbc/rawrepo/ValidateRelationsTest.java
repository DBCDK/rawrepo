/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.marcxmerge.MarcXChangeMimeType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ValidateRelationsTest {
    private final static RawRepoDAO dao = mock(RawRepoDAO.class);

    private final static int agencyIdEnrichment = 191919;
    private final static int agencyIdMarcxchange = 870970;
    private final static int agencyIdArticle = 870971;
    private final static int agencyIdLittolk = 870974;
    private final static int agencyHostPub = 870975;
    private final static int agencyIdMatVurd = 870976;
    private final static int agencyIdAuthority = 870979;
    private final static int agencyIdSimple = 190007;

    private final RecordId marcx1 = new RecordId("marcx1", agencyIdMarcxchange);
    private final RecordId marcx2 = new RecordId("marcx2", agencyIdMarcxchange);
    private final RecordId article = new RecordId("article", agencyIdArticle);
    private final RecordId hostpub = new RecordId("hostpub", agencyHostPub);
    private final RecordId littolk = new RecordId("littolk", agencyIdLittolk);
    private final RecordId authority1 = new RecordId("authority1", agencyIdAuthority);
    private final RecordId authority2 = new RecordId("authority2", agencyIdAuthority);
    private final RecordId matvurd = new RecordId("matvurd", agencyIdMatVurd);
    private final RecordId simple = new RecordId("simple", agencyIdSimple);

    private final Set<RecordId> noParents = new HashSet<>();
    private final Set<RecordId> marcXParent = new HashSet<>(Collections.singletonList(marcx1));
    private final Set<RecordId> marcXParents = new HashSet<>(Arrays.asList(marcx1, marcx2));
    private final Set<RecordId> articleParent = new HashSet<>(Collections.singletonList(article));
    private final Set<RecordId> hostpubParent = new HashSet<>(Collections.singletonList(hostpub));
    private final Set<RecordId> littolkParent = new HashSet<>(Collections.singletonList(littolk));
    private final Set<RecordId> authorityParent = new HashSet<>(Collections.singletonList(authority1));
    private final Set<RecordId> authorityParents = new HashSet<>(Arrays.asList(authority1, authority2));
    private final Set<RecordId> matvurdParent = new HashSet<>(Collections.singletonList(matvurd));
    private final Set<RecordId> simpleParent = new HashSet<>(Collections.singletonList(simple));

    @BeforeClass
    public static void before() throws Exception {
        when(dao.getMimeTypeOf(anyString(), eq(agencyIdMarcxchange))).thenReturn(MarcXChangeMimeType.MARCXCHANGE);
        when(dao.getMimeTypeOf(anyString(), eq(agencyIdArticle))).thenReturn(MarcXChangeMimeType.ARTICLE);
        when(dao.getMimeTypeOf(anyString(), eq(agencyHostPub))).thenReturn(MarcXChangeMimeType.HOSTPUB);
        when(dao.getMimeTypeOf(anyString(), eq(agencyIdLittolk))).thenReturn(MarcXChangeMimeType.LITANALYSIS);
        when(dao.getMimeTypeOf(anyString(), eq(agencyIdMatVurd))).thenReturn(MarcXChangeMimeType.MATVURD);
        when(dao.getMimeTypeOf(anyString(), eq(agencyIdAuthority))).thenReturn(MarcXChangeMimeType.AUTHORITY);
        when(dao.getMimeTypeOf(anyString(), eq(agencyIdSimple))).thenReturn(MarcXChangeMimeType.SIMPLE);
    }

    private void testInvalidParent(int childAgencyId, Set<RecordId> parentSet) {
        try {
            ValidateRelations.validate(dao, new RecordId("child", childAgencyId), parentSet);
            Assert.fail("Expected RawRepoException");
        } catch (RawRepoException e) {
            // Ok, do nothing
        }
    }

    @Test
    public void makeValidatorMarcXchange_Valid() throws RawRepoException {
        ValidateRelations.validate(dao, new RecordId("child", agencyIdArticle), noParents);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdArticle), marcXParent);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdArticle), authorityParent);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdArticle), authorityParents);
    }

    @Test
    public void makeValidatorMarcXchange_Invalid() {
        testInvalidParent(agencyIdMarcxchange, marcXParents);
        testInvalidParent(agencyIdMarcxchange, articleParent);
        testInvalidParent(agencyIdMarcxchange, hostpubParent);
        testInvalidParent(agencyIdMarcxchange, littolkParent);
        testInvalidParent(agencyIdMarcxchange, matvurdParent);
        testInvalidParent(agencyIdMarcxchange, simpleParent);
    }

    @Test
    public void makeValidatorArticle_Valid() throws RawRepoException {
        ValidateRelations.validate(dao, new RecordId("child", agencyIdArticle), noParents);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdArticle), marcXParent);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdArticle), articleParent);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdArticle), authorityParent);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdArticle), authorityParents);
    }

    @Test
    public void makeValidatorArticle_Invalid() {
        testInvalidParent(agencyIdArticle, marcXParents);
        testInvalidParent(agencyIdArticle, hostpubParent);
        testInvalidParent(agencyIdArticle, littolkParent);
        testInvalidParent(agencyIdArticle, matvurdParent);
        testInvalidParent(agencyIdArticle, simpleParent);
    }

    @Test
    public void makeValidatorAuthority_Valid() throws RawRepoException {
        ValidateRelations.validate(dao, new RecordId("child", agencyIdAuthority), noParents);
    }

    @Test
    public void makeValidatorAuthority_Invalid() {
        testInvalidParent(agencyIdAuthority, marcXParent);
        testInvalidParent(agencyIdAuthority, marcXParents);
        testInvalidParent(agencyIdAuthority, articleParent);
        testInvalidParent(agencyIdAuthority, hostpubParent);
        testInvalidParent(agencyIdAuthority, littolkParent);
        testInvalidParent(agencyIdAuthority, authorityParent);
        testInvalidParent(agencyIdAuthority, matvurdParent);
        testInvalidParent(agencyIdAuthority, simpleParent);
    }

    @Test
    public void makeValidatorHostPub_Valid() throws RawRepoException {
        ValidateRelations.validate(dao, new RecordId("child", agencyHostPub), noParents);
        ValidateRelations.validate(dao, new RecordId("child", agencyHostPub), marcXParent);
    }

    @Test
    public void makeValidatorHostPub_Invalid() {
        testInvalidParent(agencyHostPub, articleParent);
        testInvalidParent(agencyHostPub, authorityParent);
        testInvalidParent(agencyHostPub, authorityParents);
        testInvalidParent(agencyHostPub, marcXParents);
        testInvalidParent(agencyHostPub, hostpubParent);
        testInvalidParent(agencyHostPub, littolkParent);
        testInvalidParent(agencyHostPub, matvurdParent);
        testInvalidParent(agencyHostPub, simpleParent);
    }

    @Test
    public void makeValidatorLitAnalysis_Valid() throws RawRepoException {
        ValidateRelations.validate(dao, new RecordId("child", agencyIdLittolk), noParents);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdLittolk), marcXParent);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdLittolk), articleParent);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdLittolk), authorityParent);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdLittolk), authorityParents);
    }

    @Test
    public void makeValidatorLitAnalysis_Invalid() {
        testInvalidParent(agencyIdLittolk, marcXParents);
        testInvalidParent(agencyIdLittolk, hostpubParent);
        testInvalidParent(agencyIdLittolk, littolkParent);
        testInvalidParent(agencyIdLittolk, matvurdParent);
        testInvalidParent(agencyIdLittolk, simpleParent);
    }

    @Test
    public void validateMatVurd_Valid() throws RawRepoException {
        ValidateRelations.validate(dao, new RecordId("child", agencyIdMatVurd), noParents);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdMatVurd), marcXParent);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdMatVurd), marcXParents);
    }

    @Test
    public void validateMatVurd_Invalid() {
        testInvalidParent(agencyIdMatVurd, articleParent);
        testInvalidParent(agencyIdMatVurd, hostpubParent);
        testInvalidParent(agencyIdMatVurd, littolkParent);
        testInvalidParent(agencyIdMatVurd, authorityParent);
        testInvalidParent(agencyIdMatVurd, matvurdParent);
        testInvalidParent(agencyIdMatVurd, simpleParent);
    }

    @Test
    public void validateSimple_Valid() throws RawRepoException {
        ValidateRelations.validate(dao, new RecordId("child", agencyIdSimple), noParents);
    }

    @Test
    public void validateSimple_Invalid() {
        testInvalidParent(agencyIdSimple, marcXParent);
        testInvalidParent(agencyIdSimple, marcXParents);
        testInvalidParent(agencyIdSimple, articleParent);
        testInvalidParent(agencyIdSimple, hostpubParent);
        testInvalidParent(agencyIdSimple, littolkParent);
        testInvalidParent(agencyIdSimple, authorityParent);
        testInvalidParent(agencyIdSimple, matvurdParent);
        testInvalidParent(agencyIdSimple, simpleParent);
    }

    @Test
    public void makeValidatorEnrichment_Valid() throws RawRepoException {
        ValidateRelations.validate(dao, new RecordId("child", agencyIdEnrichment), marcXParent);

        // This should actually fail but enrichment validation should be looked at in general at some point
        ValidateRelations.validate(dao, new RecordId("child", agencyIdEnrichment), marcXParents);

        ValidateRelations.validate(dao, new RecordId("child", agencyIdEnrichment), articleParent);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdEnrichment), hostpubParent);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdEnrichment), littolkParent);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdEnrichment), authorityParent);

        // This should actually fail but enrichment validation should be looked at in general at some point
        ValidateRelations.validate(dao, new RecordId("child", agencyIdEnrichment), authorityParents);

        ValidateRelations.validate(dao, new RecordId("child", agencyIdEnrichment), matvurdParent);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdEnrichment), simpleParent);
    }

    @Test
    public void makeValidatorEnrichment_Invalid() throws RawRepoException {
        // TODO Validation of enrichment relations need a good rethinking
    }

}
