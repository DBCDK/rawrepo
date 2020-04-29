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
    private final static int agencyIdMatVurd = 870976;
    private final static int agencyIdAuthority = 870979;

    private final RecordId marcx1 = new RecordId("marcx1", agencyIdMarcxchange);
    private final RecordId marcx2 = new RecordId("marcx2", agencyIdMarcxchange);
    private final RecordId article = new RecordId("article", agencyIdArticle);
    private final RecordId littolk = new RecordId("littolk", agencyIdLittolk);
    private final RecordId authority1 = new RecordId("authority1", agencyIdAuthority);
    private final RecordId authority2 = new RecordId("authority2", agencyIdAuthority);
    private final RecordId matvurd = new RecordId("matvurd", agencyIdMatVurd);

    private final Set<RecordId> noParents = new HashSet<>();
    private final Set<RecordId> marcXParent = new HashSet<>(Collections.singletonList(marcx1));
    private final Set<RecordId> marcXParents = new HashSet<>(Arrays.asList(marcx1, marcx2));
    private final Set<RecordId> articleParent = new HashSet<>(Collections.singletonList(article));
    private final Set<RecordId> littolkParent = new HashSet<>(Collections.singletonList(littolk));
    private final Set<RecordId> authorityParent = new HashSet<>(Collections.singletonList(authority1));
    private final Set<RecordId> authorityParents = new HashSet<>(Arrays.asList(authority1, authority2));
    private final Set<RecordId> matvurdParent = new HashSet<>(Collections.singletonList(matvurd));

    @BeforeClass
    public static void before() throws Exception {
        when(dao.getMimeTypeOf(anyString(), eq(agencyIdMarcxchange))).thenReturn(MarcXChangeMimeType.MARCXCHANGE);
        when(dao.getMimeTypeOf(anyString(), eq(agencyIdArticle))).thenReturn(MarcXChangeMimeType.ARTICLE);
        when(dao.getMimeTypeOf(anyString(), eq(agencyIdLittolk))).thenReturn(MarcXChangeMimeType.LITANALYSIS);
        when(dao.getMimeTypeOf(anyString(), eq(agencyIdMatVurd))).thenReturn(MarcXChangeMimeType.MATVURD);
        when(dao.getMimeTypeOf(anyString(), eq(agencyIdAuthority))).thenReturn(MarcXChangeMimeType.AUTHORITY);
    }

    private void testInvalidParent(int childAgencyId, Set<RecordId> parentSet) throws RawRepoException {
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
    public void makeValidatorMarcXchange_Invalid() throws RawRepoException {
        testInvalidParent(agencyIdMarcxchange, marcXParents);
        testInvalidParent(agencyIdMarcxchange, articleParent);
        testInvalidParent(agencyIdMarcxchange, littolkParent);
        testInvalidParent(agencyIdMarcxchange, matvurdParent);
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
    public void makeValidatorArticle_Invalid() throws RawRepoException {
        testInvalidParent(agencyIdArticle, marcXParents);
        testInvalidParent(agencyIdArticle, littolkParent);
        testInvalidParent(agencyIdArticle, matvurdParent);
    }

    @Test
    public void makeValidatorAuthority_Valid() throws RawRepoException {
        ValidateRelations.validate(dao, new RecordId("child", agencyIdAuthority), noParents);
    }

    @Test
    public void makeValidatorAuthority_Invalid() throws RawRepoException {
        testInvalidParent(agencyIdAuthority, marcXParent);
        testInvalidParent(agencyIdAuthority, marcXParents);
        testInvalidParent(agencyIdAuthority, articleParent);
        testInvalidParent(agencyIdAuthority, littolkParent);
        testInvalidParent(agencyIdAuthority, authorityParent);
        testInvalidParent(agencyIdAuthority, matvurdParent);
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
    public void makeValidatorLitAnalysis_Invalid() throws RawRepoException {
        testInvalidParent(agencyIdLittolk, marcXParents);
        testInvalidParent(agencyIdLittolk, littolkParent);
        testInvalidParent(agencyIdLittolk, matvurdParent);
    }

    @Test
    public void validateMatVurd_Valid() throws RawRepoException {
        ValidateRelations.validate(dao, new RecordId("child", agencyIdMatVurd), noParents);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdMatVurd), marcXParent);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdMatVurd), marcXParents);
    }

    @Test
    public void validateMatVurd_Invalid() throws RawRepoException {
        testInvalidParent(agencyIdMatVurd, articleParent);
        testInvalidParent(agencyIdMatVurd, littolkParent);
        testInvalidParent(agencyIdMatVurd, authorityParent);
        testInvalidParent(agencyIdMatVurd, matvurdParent);
    }

    @Test
    public void makeValidatorEnrichment_Valid() throws RawRepoException {
        ValidateRelations.validate(dao, new RecordId("child", agencyIdEnrichment), marcXParent);

        // This should actually fail but enrichment validation should be looked at in general at some point
        ValidateRelations.validate(dao, new RecordId("child", agencyIdEnrichment), marcXParents);

        ValidateRelations.validate(dao, new RecordId("child", agencyIdEnrichment), articleParent);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdEnrichment), littolkParent);
        ValidateRelations.validate(dao, new RecordId("child", agencyIdEnrichment), authorityParent);

        // This should actually fail but enrichment validation should be looked at in general at some point
        ValidateRelations.validate(dao, new RecordId("child", agencyIdEnrichment), authorityParents);

        ValidateRelations.validate(dao, new RecordId("child", agencyIdEnrichment), matvurdParent);
    }

    @Test
    public void makeValidatorEnrichment_Invalid() throws RawRepoException {
        // TODO Validation of enrichment relations need a good rethinking
    }

}
