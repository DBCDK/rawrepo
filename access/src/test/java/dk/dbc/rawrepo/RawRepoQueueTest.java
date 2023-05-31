package dk.dbc.rawrepo;

import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.vipcore.exception.VipCoreException;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static dk.dbc.marcxmerge.MarcXChangeMimeType.ARTICLE;
import static dk.dbc.marcxmerge.MarcXChangeMimeType.AUTHORITY;
import static dk.dbc.marcxmerge.MarcXChangeMimeType.ENRICHMENT;
import static dk.dbc.marcxmerge.MarcXChangeMimeType.LITANALYSIS;
import static dk.dbc.marcxmerge.MarcXChangeMimeType.MATVURD;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author DBC {@literal <dbc.dk>}
 */
public class RawRepoQueueTest {

    public RawRepoQueueTest() {
    }

    @Test
    public void testMock() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .relationHints(300100, 300000, 870970)
                .doesNotUseCommon(999999)
                .addDeleted("999999:a-h1")
                .add("870970:a-h1")
                .add("191919:a-h1:870970")
                .add("300000:a-h1:870970")
                .add("870970:a-s1:870970:a-h1")
                .add("870971:anm#" + ARTICLE)
                .build();

        recordSetIs(dao.getRelationsParents(recordFromString("870970:a-s1")),
                "870970:a-h1");

        recordSetIs(dao.getRelationsChildren(recordFromString("870970:a-h1")),
                "870970:a-s1");

        recordSetIs(dao.getRelationsSiblingsToMe(recordFromString("870970:a-h1")),
                "191919:a-h1", "300000:a-h1");

        recordSetIs(dao.getRelationsSiblingsFromMe(recordFromString("191919:a-h1")),
                "870970:a-h1");

        assertThat(dao.getMimeTypeOf("a-h1", 191919), is(ENRICHMENT));
        assertThat(dao.getMimeTypeOf("anm", 870971), is(ARTICLE));

        assertThat(dao.findParentRelationAgency("a-h1", 300100), is(300000));
    }

    @Test
    public void testEnqueueOne() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870970:R")
                .build();
        dao.changedRecord("PRO", recordFromString("870970:R"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "870970:R:CL");
    }

    @Test
    public void testEnqueueDeleted() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .addDeleted("870970:R")
                .build();
        dao.changedRecord("PRO", recordFromString("870970:R"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "870970:R:CL");
    }

    @Test
    public void testEnqueueMissing() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .build();
        dao.changedRecord("PRO", recordFromString("870970:R"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "870970:R:CL");
    }

    @Test
    public void testEnqueueMissingNoCommon() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .doesNotUseCommon(870970)
                .build();

        Assertions.assertThrows(RawRepoExceptionRecordNotFound.class, () -> dao.changedRecord("PRO", recordFromString("870970:R")));
    }

    @Test
    public void testEnqueueSibling() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .relationHints(300100, 870970, 300000)
                .add("870970:R").add("191919:R:870970").add("300000:R:+").add("300100:R:+")
                .build();
        dao.changedRecord("PRO", recordFromString("870970:R"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "870970:R:CL",
                "191919:R:-L",
                "300000:R:-L",
                "300100:R:-L");
    }

    @Test
    public void testEnqueueChildren() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870970:H")
                .add("870970:S:870970:H")
                .add("870970:B:870970:S")
                .build();
        dao.changedRecord("PRO", recordFromString("870970:H"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "870970:H:C-",
                "870970:S:--",
                "870970:B:-L");
    }

    @Test
    public void testEnqueueChildrenParentHasSiblings() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870970:H").add("191919:H:870970")
                .add("870970:S:870970:H")
                .add("870970:B:870970:S")
                .build();
        dao.changedRecord("PRO", recordFromString("870970:H"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "870970:H:C-",
                "191919:H:--",
                "870970:S:--",
                "191919:S:--",
                "870970:B:-L",
                "191919:B:-L");
    }

    @Test
    public void testEnqueueSectionParentHasSiblings() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870970:H").add("191919:H:870970")
                .add("870970:S:870970:H")
                .add("870970:B:870970:S")
                .build();
        dao.changedRecord("PRO", recordFromString("870970:S"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "870970:S:C-",
                "191919:S:--",
                "870970:B:-L",
                "191919:B:-L");
    }

    @Test
    public void testEnqueueChildParentHasSiblings() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870970:H").add("191919:H:870970")
                .add("870970:S:870970:H")
                .add("870970:B:870970:S")
                .build();
        dao.changedRecord("PRO", recordFromString("870970:B"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "870970:B:CL",
                "191919:B:-L");
    }

    @Test
    public void testEnqueueParentsSibling() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870970:H").add("191919:H:870970")
                .add("870970:S:870970:H")
                .add("870970:B:870970:S")
                .build();
        dao.changedRecord("PRO", recordFromString("191919:H"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "191919:H:C-",
                "191919:S:--",
                "191919:B:-L");
    }

    @Test
    public void testEnqueueForeignChildrenParentHasSiblings() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870970:H").add("191919:H:870970")
                .add("870970:S:870970:H").add("555555:S:870970:H")
                .add("870970:B:870970:S").add("555555:B:555555:S")
                .build();
        dao.changedRecord("PRO", recordFromString("870970:H"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "870970:H:C-",
                "191919:H:--",
                "870970:S:--",
                "191919:S:--",
                "870970:B:-L",
                "191919:B:-L",
                "555555:S:--",
                "555555:B:-L");
    }

    @Test
    public void testEnqueueForeignChildrenParentHasSiblingsPart2() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870970:H").add("191919:H:870970")
                .add("870970:S:870970:H").add("555555:S:870970:H")
                .add("870970:B:870970:S").add("555555:B:555555:S")
                .build();
        dao.changedRecord("PRO", recordFromString("555555:S"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "555555:S:C-",
                "555555:B:-L");
    }

    @Test
    public void testEnqueueSiblingWithEnrichment() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870970:H")
                .add("870970:S:870970:H").add("300000:S:+")
                .add("870970:B:870970:S").add("300000:B:+").add("300100:B:+")
                .build();
        dao.changedRecord("PRO", recordFromString("300000:S"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "300000:S:C-",
                "300100:B:-L",
                "300000:B:-L");
    }

    @Test
    public void testEnqueueSiblingWithEnrichment2() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870970:H")
                .add("870970:S1:870970:H")
                .add("200000:S1:870970")
                .add("200100:S1:870970")
                .add("870970:B11:870970:S1")
                .add("870970:B12:870970:S1")
                .add("870970:S2:870970:H")
                .add("200000:S2:870970").add("200100:S2:+").add("200101:S2:+")
                .add("870970:B21:870970:S2")
                .add("870970:B22:870970:S2")
                .build();
        dao.changedRecord("PRO", recordFromString("200000:S1"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "200000:S1:C-",
                "200000:B11:-L",
                "200000:B12:-L");
    }

    @Test
    public void testEnqueueSiblingWithEnrichment3() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870970:H")
                .add("200000:H:870970")
                .add("200100:H:870970")
                .add("870970:S1:870970:H")
                .add("870970:B11:870970:S1")
                .add("870970:B12:870970:S1")
                .add("870970:S2:870970:H")
                .add("870970:B21:870970:S2")
                .add("870970:B22:870970:S2")
                .build();
        dao.changedRecord("PRO", recordFromString("200000:H"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "200000:H:C-",
                "200000:S1:--",
                "200000:B11:-L",
                "200000:B12:-L",
                "200000:S2:--",
                "200000:B21:-L",
                "200000:B22:-L");
    }

    @Test
    public void testEnqueueSiblingWithEnrichment4() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870970:H")
                .add("200000:H:870970")
                .add("870970:S1:870970:H")
                .add("200000:S1-200:200000:H")
                .add("870970:B11:870970:S1")
                .add("200000:B11-200:200000:S1-200")
                .add("870970:B12:870970:S1")
                .add("870970:S2:870970:H")
                .add("870970:B21:870970:S2")
                .add("870970:B22:870970:S2")
                .build();
        dao.changedRecord("PRO", recordFromString("200000:H"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "200000:H:C-",
                "200000:S1-200:--",
                "200000:B11-200:-L");
    }

    @Test
    public void testEnqueueSiblingWithParentSiblings() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870970:H").add("300000:H:+").add("300100:H:+")
                .add("870970:S1:870970:H")
                .add("870970:B11:870970:S1").add("300000:B11:+")
                .build();
        dao.changedRecord("PRO", recordFromString("300000:B11"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "300000:B11:CL",
                "300100:B11:-L");
    }

    @Test
    public void testEnqueueChildrenArticle() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870970:H")
                .add("870970:S:870970:H")
                .add("870970:B:870970:S")
                .add("870971:A:870970:B#" + ARTICLE)
                .build();
        dao.changedRecord("PRO", recordFromString("870970:H"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "870970:H:C-",
                "870970:S:--",
                "870970:B:-L",
                "870971:A:-L");
    }

    @Test
    public void testEnqueueChildrenArticleSection() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870970:H")
                .add("870970:S:870970:H")
                .add("870971:A:870970:S#" + ARTICLE)
                .add("870970:B:870970:S")
                .build();
        dao.changedRecord("PRO", recordFromString("870970:H"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "870970:H:C-",
                "870970:S:--",
                "870970:B:-L",
                "870971:A:-L");
    }

    @Test
    public void testEnqueueArticle() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870970:H")
                .add("870970:S:870970:H")
                .add("870970:B:870970:S")
                .add("870971:A:870970:B#" + ARTICLE)
                .build();
        dao.changedRecord("PRO", recordFromString("870971:A"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "870971:A:CL");
    }

    @Test
    public void testEnqueueArticleHierarchy() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();
        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870970:H")
                .add("870970:S:870970:H")
                .add("870970:B:870970:S")
                .add("870971:A1:870970:B#" + ARTICLE)
                .add("870971:A2:870971:A1#" + ARTICLE)
                .build();
        dao.changedRecord("PRO", recordFromString("870971:A1"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "870971:A1:C-",
                "870971:A2:-L");
    }

    @Test
    public void testEnqueueArticleEnrichment() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();

        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870979:68369444#" + AUTHORITY)
                .add("870970:29778795:870979:68369444")
                .add("191919:29778795:870970:29778795#" + ENRICHMENT)
                .add("715700:29778795:870970:29778795#" + ENRICHMENT)
                .add("870971:126341873:870970:29778795#" + ARTICLE)
                .add("191919:126341873:870971:126341873#" + ENRICHMENT)
                .build();

        dao.changedRecord("PRO", recordFromString("191919:126341873"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "191919:126341873:CL");
    }

    @Test
    public void testEnqueueMatVurdEnrichment() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();

        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870979:68369444#" + AUTHORITY)
                .add("870970:29778795:870979:68369444")
                .add("191919:29778795:870970:29778795#" + ENRICHMENT)
                .add("715700:29778795:870970:29778795#" + ENRICHMENT)
                .add("870976:126341873:870970:29778795#" + MATVURD)
                .add("191919:126341873:870976:126341873#" + ENRICHMENT)
                .build();

        dao.changedRecord("PRO", recordFromString("191919:126341873"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "191919:126341873:CL");
    }

    @Test
    public void testEnqueueLitAnalysisEnrichment() throws Exception {
        HashMap<String, AtomicInteger> enqueued = new HashMap<>();

        RawRepoDAO dao = RawRepoMock.builder(enqueued)
                .add("870979:68369444#" + AUTHORITY)
                .add("870970:29778795:870979:68369444")
                .add("191919:29778795:870970:29778795#" + ENRICHMENT)
                .add("715700:29778795:870970:29778795#" + ENRICHMENT)
                .add("870974:126341873:870970:29778795#" + LITANALYSIS)
                .add("191919:126341873:870974:126341873#" + ENRICHMENT)
                .build();

        dao.changedRecord("PRO", recordFromString("191919:126341873"));
        System.out.println("enqueued = " + enqueued);
        enqueuedIs(enqueued,
                "191919:126341873:CL");
    }

    /*
     *     ______          __
     *    /_  __/__  _____/ /_
     *     / / / _ \/ ___/ __/
     *    / / /  __(__  ) /_
     *   /_/  \___/____/\__/
     *
     */
    private static void enqueuedIs(HashMap<String, AtomicInteger> enqueued, String... strings) {
        setIs(enqueued.keySet(), strings);
        for (String string : strings) {
            AtomicInteger count = enqueued.get(string);
            if (count.get() != 1) {
                fail(string + " enqueued " + count + " times");
            }
        }
    }

    static RecordId recordFromString(String agencyBibliographicRecordId) {
        String[] parts = agencyBibliographicRecordId.split(":");
        return new RecordId(parts[1], Integer.parseInt(parts[0])) {
            @Override
            public String toString() {
                return agencyBibliographicRecordId;
            }
        };
    }

    private static String recordToString(RecordId recordId) {
        String bibliographicRecordId = recordId.getBibliographicRecordId();
        int agencyId = recordId.getAgencyId();
        return agencyId + ":" + bibliographicRecordId;
    }

    private static void recordSetIs(Set<RecordId> records, String... values) {
        setIs(records.stream()
                .map(RawRepoQueueTest::recordToString)
                .collect(toSet()), values);
    }

    private static void setIs(Set<String> set, String... values) {
        assertThat(set, IsCollectionContaining.hasItems(values));
        assertThat(values.length, is(set.size()));
    }

}

/*
 *       __  ___           __
 *      /  |/  /___  _____/ /__
 *     / /|_/ / __ \/ ___/ //_/
 *    / /  / / /_/ / /__/ ,<
 *   /_/  /_/\____/\___/_/|_|
 *
 */
class RawRepoMock {

    private final Set<String> recordExists;
    private final Set<String> recordExistsMaybeDeleted;
    private final Map<String, Set<Integer>> agenciesForRecord;
    private final Map<String, Set<String>> outboundRelation;
    private final Map<String, Set<String>> inboundRelation;
    private final Map<String, String> mimetype;
    private final Map<Integer, List<Integer>> relationHints;
    private final Set<Integer> doesNotUseCommon;
    private final Map<String, AtomicInteger> enqueued;
    private int lastAddedAgency;

    private RawRepoMock(HashMap<String, AtomicInteger> enqueued) {
        this.recordExists = new HashSet<>();
        this.recordExistsMaybeDeleted = new HashSet<>();
        this.agenciesForRecord = new HashMap<>();
        this.outboundRelation = new HashMap<>();
        this.inboundRelation = new HashMap<>();
        this.mimetype = new HashMap<>();
        this.doesNotUseCommon = new HashSet<>();
        this.relationHints = new HashMap<>();
        this.enqueued = enqueued;
        this.lastAddedAgency = 0;
    }

    static RawRepoMock builder(HashMap<String, AtomicInteger> enqueued) {
        return new RawRepoMock(enqueued);
    }

    RawRepoMock relationHints(int agencyId, Integer... order) {
        relationHints.put(agencyId, Arrays.asList(order));
        return this;
    }

    RawRepoMock doesNotUseCommon(int agencyId) {
        doesNotUseCommon.add(agencyId);
        return this;
    }

    RawRepoMock add(String rec) {
        String[] a = rec.split("#", 2);
        String recordMimetype = MarcXChangeMimeType.MARCXCHANGE;
        if (a.length == 2) {
            recordMimetype = a[1];
            rec = a[0];
        }

        String[] parts = rec.split(":", 3);
        int agencyId = Integer.parseInt(parts[0]);
        String bibliographicRecordId = parts[1];
        String id = agencyId + ":" + bibliographicRecordId;
        recordExists.add(id);
        agenciesForRecord.computeIfAbsent(bibliographicRecordId, s -> new HashSet<>()).add(agencyId);
        mimetype.put(id, recordMimetype);
        if (parts.length == 3) {
            Arrays.stream(parts[2].split(","))
                    .map(s -> s.split(":", 2))
                    .forEach(recParts -> {
                        String referBibliographicRecordId = bibliographicRecordId;
                        int referAgencyId = lastAddedAgency;
                        if (recParts.length == 1) {
                            if (mimetype.get(id).equals(MarcXChangeMimeType.MARCXCHANGE)) {
                                mimetype.put(id, MarcXChangeMimeType.ENRICHMENT);
                            }
                            if (!recParts[0].equals("+")) {
                                referAgencyId = Integer.parseInt(recParts[0]);
                            }
                        } else {
                            referAgencyId = Integer.parseInt(recParts[0]);
                            referBibliographicRecordId = recParts[1];
                        }
                        String relation = referAgencyId + ":" + referBibliographicRecordId;
                        outboundRelation.computeIfAbsent(id, r -> new HashSet<>()).add(relation);
                        inboundRelation.computeIfAbsent(relation, r -> new HashSet<>()).add(id);
                    });
        }
        lastAddedAgency = agencyId;
        return this;
    }

    RawRepoMock addDeleted(String rec) {
        String[] parts = rec.split(":");
        int agencyId = Integer.parseInt(parts[0]);
        String bibliographicRecordId = parts[1];
        String id = agencyId + ":" + bibliographicRecordId;
        recordExistsMaybeDeleted.add(id);
        agenciesForRecord.computeIfAbsent(bibliographicRecordId, s -> new HashSet<>()).add(agencyId);
        mimetype.put(id, MarcXChangeMimeType.MARCXCHANGE);
        return this;
    }

    private Set<RecordId> getRecordsWithoutBibrecidMatch(InvocationOnMock invocation, Map<String, Set<String>> ioBoundRelation) {
        RecordId recordId = (RecordId) invocation.getArguments()[0];
        String bibliographicRecordId = recordId.getBibliographicRecordId();
        int agencyId = recordId.getAgencyId();
        String id = agencyId + ":" + bibliographicRecordId;
        Set<String> set = ioBoundRelation.get(id);
        if (set == null) {
            return Collections.emptySet();
        }
        return set.stream()
                .filter(child -> !child.endsWith(":" + bibliographicRecordId))
                .map(RawRepoQueueTest::recordFromString)
                .collect(toSet());
    }

    private Map<RecordId, String> getMimeTypeOfListWithRecidMatch(InvocationOnMock invocation, Map<String, Set<String>> ioBoundRelation) throws RawRepoException {
        final Set<RecordId> input = invocation.getArgument(0);
        final Map<RecordId, String> result = new HashMap<>();
        for (RecordId recordId : input) {
            String bibliographicRecordId = recordId.getBibliographicRecordId();
            int agencyId = recordId.getAgencyId();
            String type = mimetype.get(agencyId + ":" + bibliographicRecordId);
            if (type == null) {
                throw new RawRepoExceptionRecordNotFound();
            } else {
                result.put(new RecordId(bibliographicRecordId, agencyId), type);
            }
        }
        return result;
    }

    private Set<RecordId> getRecordsWithBibrecidMatch(InvocationOnMock invocation, Map<String, Set<String>> ioBoundRelation) {
        RecordId recordId = (RecordId) invocation.getArguments()[0];
        String bibliographicRecordId = recordId.getBibliographicRecordId();
        int agencyId = recordId.getAgencyId();
        String id = agencyId + ":" + bibliographicRecordId;
        Set<String> set = ioBoundRelation.get(id);
        if (set == null) {
            return Collections.emptySet();
        }
        return set.stream()
                .filter(child -> child.endsWith(":" + bibliographicRecordId))
                .map(RawRepoQueueTest::recordFromString)
                .collect(toSet());
    }

    RawRepoDAO build() throws RawRepoException, VipCoreException {
        RawRepoDAO rawrepo = mock(RawRepoDAO.class);
        when(rawrepo.recordExists(anyString(), anyInt())).then((Answer<Boolean>) invocation -> {
            String bibliographicRecordId = (String) invocation.getArguments()[0];
            int agencyId = (int) invocation.getArguments()[1];
            return recordExists.contains(agencyId + ":" + bibliographicRecordId);
        });
        when(rawrepo.recordExistsMaybeDeleted(anyString(), anyInt())).then((Answer<Boolean>) invocation -> {
            String bibliographicRecordId = (String) invocation.getArguments()[0];
            int agencyId = (int) invocation.getArguments()[1];
            return recordExists.contains(agencyId + ":" + bibliographicRecordId) ||
                    recordExistsMaybeDeleted.contains(agencyId + ":" + bibliographicRecordId);
        });
        when(rawrepo.getRelationsChildren(any(RecordId.class))).thenAnswer((Answer<Set<RecordId>>) invocation -> getRecordsWithoutBibrecidMatch(invocation, inboundRelation));
        when(rawrepo.getRelationsParents(any(RecordId.class))).then((Answer<Set<RecordId>>) invocation -> getRecordsWithoutBibrecidMatch(invocation, outboundRelation));
        when(rawrepo.getRelationsSiblingsFromMe(any(RecordId.class))).then((Answer<Set<RecordId>>) invocation -> getRecordsWithBibrecidMatch(invocation, outboundRelation));
        when(rawrepo.getRelationsSiblingsToMe(any(RecordId.class))).then((Answer<Set<RecordId>>) invocation -> getRecordsWithBibrecidMatch(invocation, inboundRelation));
        when(rawrepo.getMimeTypeOfList(any(Set.class))).then((Answer<Map<RecordId, String>>) invocation -> getMimeTypeOfListWithRecidMatch(invocation, inboundRelation));
        when(rawrepo.getMimeTypeOf(anyString(), anyInt())).then((Answer<String>) invocation -> {
            String bibliographicRecordId = (String) invocation.getArguments()[0];
            int agencyId = (int) invocation.getArguments()[1];
            String type = mimetype.get(agencyId + ":" + bibliographicRecordId);
            if (type == null) {
                throw new RawRepoExceptionRecordNotFound();
            }
            return type;
        });
        when(rawrepo.allAgenciesForBibliographicRecordId(anyString())).then((Answer<Set<Integer>>) invocation -> {
            String bibliographicRecordId = (String) invocation.getArguments()[0];
            Set<Integer> set = agenciesForRecord.get(bibliographicRecordId);
            if (set == null) {
                throw new RawRepoExceptionRecordNotFound();
            }
            return set;
        });

        doAnswer((Answer<Void>) this::enqueue).when(rawrepo).enqueue(any(RecordId.class), anyString(), anyBoolean(), anyBoolean());
        doAnswer((Answer<Void>) this::enqueue).when(rawrepo).enqueue(any(RecordId.class), anyString(), anyBoolean(), anyBoolean(), anyInt());
        doAnswer((Answer<Void>) this::enqueueBulk).when(rawrepo).enqueueBulk(any(List.class));
        doCallRealMethod().when(rawrepo).agencyFor(anyString(), anyInt(), anyBoolean());
        doCallRealMethod().when(rawrepo).findParentRelationAgency(anyString(), anyInt());
        doCallRealMethod().when(rawrepo).findSiblingRelationAgency(anyString(), anyInt());
        doCallRealMethod().when(rawrepo).changedRecord(anyString(), any(RecordId.class));

        List<Integer> defaultList = Collections.singletonList(870970);

        RelationHintsVipCore relations = mock(RelationHintsVipCore.class);
        rawrepo.relationHints = relations;
        when(relations.usesCommonAgency(anyInt())).then((Answer<Boolean>) invocation -> {
            int agencyid = (int) invocation.getArguments()[0];
            return !doesNotUseCommon.contains(agencyid);
        });
        when(relations.get(anyInt())).then((Answer<List<Integer>>) invocation -> {
            int agencyid = (int) invocation.getArguments()[0];
            return relationHints.getOrDefault(agencyid, defaultList);
        });
        print();
        return rawrepo;
    }

    private Void enqueue(InvocationOnMock invocation) {
        Object[] arguments = invocation.getArguments();
        for (Object argument : arguments) {
            System.out.println(argument);
        }
        System.out.println(arguments[0]);
        RecordId recordId = (RecordId) arguments[0];
        System.out.println(arguments[2]);
        boolean changed = (boolean) arguments[2];
        System.out.println(arguments[3]);
        boolean leaf = (boolean) arguments[3];
        String id = String.valueOf(recordId.getAgencyId()) + ':' + recordId.getBibliographicRecordId() +
                ':' +
                (changed ? 'C' : '-') +
                (leaf ? 'L' : '-');
        enqueued.computeIfAbsent(id, s -> new AtomicInteger(0)).incrementAndGet();
        return null;
    }

    private Void enqueueBulk(InvocationOnMock invocation) {
        final Object[] arguments = invocation.getArguments();
        for (Object argument : arguments) {
            System.out.println(argument);
        }
        final List<EnqueueJob> enqueueJobList = (List<EnqueueJob>) arguments[0];
        for (EnqueueJob obj : enqueueJobList) {
            final String id = String.valueOf(obj.getJob().getAgencyId()) + ':' + obj.getJob().getBibliographicRecordId() +
                    ':' +
                    (obj.isChanged() ? 'C' : '-') +
                    (obj.isLeaf() ? 'L' : '-');
            enqueued.computeIfAbsent(id, s -> new AtomicInteger(0)).incrementAndGet();
        }
        return null;
    }


    private void print() {
        HashMap<String, String> toParent = new HashMap<>();
        for (Map.Entry<String, Set<Integer>> recordToAgency : agenciesForRecord.entrySet()) {
            String bibliographicRecordId = recordToAgency.getKey();
            String parent = null;
            for (Integer agency : recordToAgency.getValue()) {
                Set<String> set = outboundRelation.get(agency + ":" + bibliographicRecordId);
                if (set != null) {
                    Optional<String> found = set.stream().filter(s -> s.contains(":") && !s.endsWith(":" + bibliographicRecordId)).findAny();
                    if (found.isPresent()) {
                        String[] split = found.get().split(":");
                        parent = split[1];
                    }
                }
            }
            toParent.put(bibliographicRecordId, parent);
        }

        HashMap<String, String> output = new HashMap<>();
        while (!toParent.isEmpty()) {
            Optional<String> found = toParent.entrySet().stream()
                    .filter(e -> e.getValue() == null)
                    .map(Map.Entry::getKey)
                    .findAny();
            if (!found.isPresent()) {
                throw new IllegalStateException("Cannot find leaf");
            }
            String record = found.get();
            while (toParent.get(record) != null) {
                record = toParent.get(record);
            }
            StringBuilder sb = new StringBuilder();

            print(sb, toParent, record, "");
            output.put(record, sb.toString());
        }
        ArrayList<String> list = new ArrayList<>(output.keySet());
        Collections.sort(list);
        list.forEach(entry -> System.out.println(output.get(entry)));
    }

    private void print(StringBuilder sb, HashMap<String, String> toParent, String key, String indent) {
        List<String> children = toParent.entrySet().stream()
                .filter(e -> key.equals(e.getValue()))
                .map(Map.Entry::getKey).sorted().collect(toList());

        sb.append(indent).append(key);
        if (indent.endsWith(INDENT_MORE)) {
            indent = indent.substring(0, indent.length() - 3) + INDENT_GAP;
        } else if (indent.endsWith(INDENT_LAST)) {
            indent = indent.substring(0, indent.length() - 3) + INDENT_NONE;
        }
        printAgency(sb, key, indent);
        sb.append("\n");
        for (Iterator<String> iterator = children.iterator(); iterator.hasNext(); ) {
            String child = iterator.next();
            if (iterator.hasNext()) {
                print(sb, toParent, child, indent + INDENT_MORE);
            } else {
                print(sb, toParent, child, indent + INDENT_LAST);
            }
        }
        toParent.remove(key);
    }

    private void printAgency(StringBuilder out, String rec, String indent) {
        HashMap<Integer, Integer> toSibling = new HashMap<>();
        Set<Integer> set = agenciesForRecord.getOrDefault(rec, Collections.emptySet());
        set.forEach(a -> toSibling.put(a, null));
        set.stream()
                .filter(a -> outboundRelation.containsKey(a + ":" + rec))
                .forEach(a -> outboundRelation.get(a + ":" + rec).stream()
                        .filter(s -> s.endsWith(":" + rec))
                        .map(s -> s.split(":")[0])
                        .map(Integer::parseInt)
                        .forEach(i -> toSibling.put(a, i)));
        HashMap<Integer, String> output = new HashMap<>();
        while (!toSibling.isEmpty()) {
            Integer agency = toSibling.entrySet().stream()
                    .filter(e -> e.getValue() == null)
                    .limit(1)
                    .map(Map.Entry::getKey)
                    .findFirst().get();
            StringBuilder sb = new StringBuilder();

            printAgency(sb, toSibling, agency, indent + "|" + spacify(rec).substring(1));
            output.put(agency, sb.toString());
        }
        ArrayList<Integer> list = new ArrayList<>(output.keySet());
        Collections.sort(list);
        list.forEach(a -> out.append(output.get(a)));
    }

    private void printAgency(StringBuilder sb, HashMap<Integer, Integer> toSibling, Integer agency, String indent) {
        toSibling.remove(agency);
        sb.append(" << ").append(agency);
        List<Integer> children = toSibling.entrySet().stream()
                .filter(e -> agency.equals(e.getValue()))
                .map(Map.Entry::getKey)
                .collect(toList());

        if (!children.isEmpty()) {
            String align = spacify(" << " + agency);
            Collections.sort(children);
            for (Iterator<Integer> iterator = children.iterator(); iterator.hasNext(); ) {
                Integer child = iterator.next();
                printAgency(sb, toSibling, child, indent + align);
                if (iterator.hasNext()) {
                    sb.append("\n").append(indent).append(align);
                }
            }
        }
    }

    private static final String INDENT_MORE = "|- ";
    private static final String INDENT_GAP = "|  ";
    private static final String INDENT_LAST = "`- ";
    private static final String INDENT_NONE = "   ";

    private static final Pattern DOT = Pattern.compile(".");

    private static String spacify(String s) {
        return DOT.matcher(s).replaceAll(" ");
    }

}
