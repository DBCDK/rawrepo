package dk.dbc.rawrepo;

import dk.dbc.commons.jsonb.JSONBContext;
import dk.dbc.rawrepo.common.ApplicationConstants;
import dk.dbc.rawrepo.common.EnvironmentVariables;
import dk.dbc.rawrepo.dao.HoldingsItemsConnector;
import dk.dbc.rawrepo.dao.OpenAgencyConnector;
import dk.dbc.rawrepo.dao.RawRepoConnector;
import dk.dbc.rawrepo.queue.QueueJob;
import dk.dbc.rawrepo.queue.QueueProcessRequest;
import dk.dbc.rawrepo.queue.QueueProcessResponse;
import dk.dbc.rawrepo.queue.QueueProvider;
import dk.dbc.rawrepo.queue.QueueType;
import dk.dbc.rawrepo.queue.QueueValidateRequest;
import dk.dbc.rawrepo.queue.QueueValidateResponse;
import dk.dbc.rawrepo.queue.QueueWorker;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class QueueAPITest {
    private final JSONBContext jsonbContext = new JSONBContext();

    private QueueAPI getQueueBean() {
        final OpenAgencyConnector openAgency = mock(OpenAgencyConnector.class);
        final RawRepoConnector rawrepo = mock(RawRepoConnector.class);
        final HoldingsItemsConnector holdingsItemsConnector = mock(HoldingsItemsConnector.class);
        final EnvironmentVariables environmentVariables = mock(EnvironmentVariables.class);

        final QueueAPI bean = new QueueAPI();
        bean.openAgency = openAgency;
        bean.rawrepo = rawrepo;
        bean.holdingsItemsConnector = holdingsItemsConnector;
        bean.variables = environmentVariables;

        return bean;
    }

    @Test
    public void testGetProviders() throws Exception {

        final List<QueueProvider> providerList = new ArrayList<>();

        final QueueWorker worker1 = new QueueWorker("worker1", "T", "F");
        final QueueProvider providerA = new QueueProvider("PROVIDER-A");
        providerA.getWorkers().add(worker1);

        final QueueWorker worker2 = new QueueWorker("worker2", "F", "T");
        final QueueProvider providerB = new QueueProvider("PROVIDER-B");
        providerB.getWorkers().add(worker2);

        providerList.add(providerA);
        providerList.add(providerB);

        final QueueAPI bean = getQueueBean();

        when(bean.rawrepo.getProviders()).thenReturn(providerList);

        final Response response = bean.getProviders();

        assertThat("Response code OK", response.getStatus(), is(200));
        assertThat("Two providers", response.getEntity().toString(), is(
                "[{\"name\":\"PROVIDER-A\",\"workers\":[" +
                        "{\"name\":\"worker1\",\"changed\":\"T\",\"leaf\":\"F\",\"description\":null}]}," +
                        "{\"name\":\"PROVIDER-B\",\"workers\":[" +
                        "{\"name\":\"worker2\",\"changed\":\"F\",\"leaf\":\"T\",\"description\":null}]}]"));
    }

    @Test
    public void testGetCatalogingTemplateSetNotBasisMig() throws Exception {
        final QueueAPI bean = getQueueBean();

        when(bean.variables.getenv(ApplicationConstants.INSTANCE_NAME)).thenReturn("test");

        final List<QueueType> expected = new ArrayList<>();
        expected.add(QueueType.ffu());
        expected.add(QueueType.fbsRawrepo());
        expected.add(QueueType.fbsRawrepoEnrichment());
        expected.add(QueueType.fbsHoldings());
        expected.add(QueueType.fbsEverything());

        final String expectedJSON = jsonbContext.marshall(expected);

        final Response response = bean.getQueueTypes();

        assertThat("Response code 200", response.getStatus(), is(200));
        assertThat("QueueTypes when not basismig", response.getEntity(), is(expectedJSON));
    }

    @Test
    public void testGetCatalogingTemplateSetBasisMig() throws Exception {
        final QueueAPI bean = getQueueBean();

        when(bean.variables.getenv(ApplicationConstants.INSTANCE_NAME)).thenReturn("test_basismig");

        final List<QueueType> expected = new ArrayList<>();
        expected.add(QueueType.ffu());
        expected.add(QueueType.fbsRawrepo());
        expected.add(QueueType.fbsRawrepoEnrichment());
        expected.add(QueueType.fbsHoldings());
        expected.add(QueueType.fbsEverything());
        expected.add(QueueType.dbcCommon());

        final String expectedJSON = jsonbContext.marshall(expected);

        final Response response = bean.getQueueTypes();

        assertThat("Response code 200", response.getStatus(), is(200));
        assertThat("QueueTypes when basismig", response.getEntity(), is(expectedJSON));
    }

    @Test
    public void testValidateEmptyRequest() throws Exception {
        final QueueAPI bean = getQueueBean();

        final QueueValidateRequest request = new QueueValidateRequest();

        Response response = bean.validate(jsonbContext.marshall(request));

        assertThat("Response code 200", response.getStatus(), is(200));

        final QueueValidateResponse queueValidateResponse = jsonbContext.unmarshall(response.getEntity().toString(),
                QueueValidateResponse.class);

        assertThat("Not validated", queueValidateResponse.isValidated(), is(false));
        assertThat("Message", queueValidateResponse.getMessage(), is("Der skal angives en provider"));
    }

    @Test
    public void testValidateInvalidProvider() throws Exception {
        final QueueAPI bean = getQueueBean();

        final QueueValidateRequest request = new QueueValidateRequest();
        request.setProvider("grydesteg");

        final Response response = bean.validate(jsonbContext.marshall(request));

        assertThat("Response code 200", response.getStatus(), is(200));

        final QueueValidateResponse queueValidateResponse = jsonbContext.unmarshall(response.getEntity().toString(),
                QueueValidateResponse.class);

        assertThat("Not validated", queueValidateResponse.isValidated(), is(false));
        assertThat("Invalid provider message", queueValidateResponse.getMessage(), is("Provideren 'grydesteg' kunne ikke valideres"));
    }

    @Test
    public void testValidateNoQueueType() throws Exception {
        final QueueAPI bean = getQueueBean();

        final QueueValidateRequest request = new QueueValidateRequest();
        request.setProvider("the-real-provider");

        when(bean.rawrepo.getProviders()).thenReturn(Arrays.asList(new QueueProvider("the-real-provider")));
        final Response response = bean.validate(jsonbContext.marshall(request));

        assertThat("Response code 200", response.getStatus(), is(200));

        final QueueValidateResponse queueValidateResponse = jsonbContext.unmarshall(response.getEntity().toString(),
                QueueValidateResponse.class);

        assertThat("Not validated", queueValidateResponse.isValidated(), is(false));
        assertThat("Missing provider message", queueValidateResponse.getMessage(), is("Der skal angives en køtype"));
    }

    @Test
    public void testValidateInvalidQueueType() throws Exception {
        final QueueAPI bean = getQueueBean();

        final QueueValidateRequest request = new QueueValidateRequest();
        request.setProvider("the-real-provider");
        request.setQueueType("grydesteg");

        when(bean.rawrepo.getProviders()).thenReturn(Arrays.asList(new QueueProvider("the-real-provider")));
        final Response response = bean.validate(jsonbContext.marshall(request));

        assertThat("Response code 200", response.getStatus(), is(200));

        final QueueValidateResponse queueValidateResponse = jsonbContext.unmarshall(response.getEntity().toString(),
                QueueValidateResponse.class);

        assertThat("Not validated", queueValidateResponse.isValidated(), is(false));
        assertThat("Invalid queuetype", queueValidateResponse.getMessage(), is("Køtypen 'grydesteg' kunne ikke valideres"));
    }

    @Test
    public void testValidateNoAgency() throws Exception {
        final QueueAPI bean = getQueueBean();

        final QueueValidateRequest request = new QueueValidateRequest();
        request.setProvider("the-real-provider");
        request.setQueueType("ffu");

        when(bean.rawrepo.getProviders()).thenReturn(Arrays.asList(new QueueProvider("the-real-provider")));
        final Response response = bean.validate(jsonbContext.marshall(request));

        assertThat("Response code 200", response.getStatus(), is(200));

        final QueueValidateResponse queueValidateResponse = jsonbContext.unmarshall(response.getEntity().toString(),
                QueueValidateResponse.class);

        assertThat("Not validated", queueValidateResponse.isValidated(), is(false));
        assertThat("Missing agency", queueValidateResponse.getMessage(), is("Der skal angives mindst ét biblioteksnummer"));
    }

    @Test
    public void testValidateSpaceAsAgency() throws Exception {
        final QueueAPI bean = getQueueBean();

        final QueueValidateRequest request = new QueueValidateRequest();
        request.setProvider("the-real-provider");
        request.setQueueType("ffu");
        request.setAgencyText(" ");

        when(bean.rawrepo.getProviders()).thenReturn(Arrays.asList(new QueueProvider("the-real-provider")));
        final Response response = bean.validate(jsonbContext.marshall(request));

        assertThat("Response code 200", response.getStatus(), is(200));

        final QueueValidateResponse queueValidateResponse = jsonbContext.unmarshall(response.getEntity().toString(),
                QueueValidateResponse.class);

        assertThat("Not validated", queueValidateResponse.isValidated(), is(false));
        assertThat("Missing agency", queueValidateResponse.getMessage(), is("Der skal angives mindst ét biblioteksnummer"));
    }

    @Test
    public void testValidateCommasAsAgency() throws Exception {
        final QueueAPI bean = getQueueBean();

        final QueueValidateRequest request = new QueueValidateRequest();
        request.setProvider("the-real-provider");
        request.setQueueType("ffu");
        request.setAgencyText(" ,,,,, \n");

        when(bean.rawrepo.getProviders()).thenReturn(Arrays.asList(new QueueProvider("the-real-provider")));
        final Response response = bean.validate(jsonbContext.marshall(request));

        assertThat("Response code 200", response.getStatus(), is(200));

        final QueueValidateResponse queueValidateResponse = jsonbContext.unmarshall(response.getEntity().toString(),
                QueueValidateResponse.class);

        assertThat("Not validated", queueValidateResponse.isValidated(), is(false));
        assertThat("Missing agency", queueValidateResponse.getMessage(), is("Der skal angives mindst ét biblioteksnummer"));
    }

    @Test
    public void testValidateInvalidAgencyIds() throws Exception {
        final QueueAPI bean = getQueueBean();

        final QueueValidateRequest request = new QueueValidateRequest();
        request.setProvider("the-real-provider");
        request.setQueueType("ffu");
        request.setAgencyText("123456 \n654321");

        Set<String> agencies = new HashSet<>();
        agencies.add("111111");

        when(bean.rawrepo.getProviders()).thenReturn(Arrays.asList(new QueueProvider("the-real-provider")));
        when(bean.openAgency.getLibrariesByCatalogingTemplateSet("ffu")).thenReturn(agencies);
        final Response response = bean.validate(jsonbContext.marshall(request));

        assertThat("Response code 200", response.getStatus(), is(200));

        final QueueValidateResponse queueValidateResponse = jsonbContext.unmarshall(response.getEntity().toString(),
                QueueValidateResponse.class);

        assertThat("Not validated", queueValidateResponse.isValidated(), is(false));
        assertThat("Invalid agency - first element", queueValidateResponse.getMessage(), is("Biblioteksnummeret '123456' tilhører ikke biblioteksgruppen ffu"));
    }

    @Test
    public void testValidateMixOfAgencies() throws Exception {
        final QueueAPI bean = getQueueBean();

        final QueueValidateRequest request = new QueueValidateRequest();
        request.setProvider("the-real-provider");
        request.setQueueType("ffu");
        request.setAgencyText("111111 \n654321");

        Set<String> agencies = new HashSet<>();
        agencies.add("111111");

        when(bean.rawrepo.getProviders()).thenReturn(Arrays.asList(new QueueProvider("the-real-provider")));
        when(bean.openAgency.getLibrariesByCatalogingTemplateSet("ffu")).thenReturn(agencies);
        final Response response = bean.validate(jsonbContext.marshall(request));

        assertThat("Response code 200", response.getStatus(), is(200));

        final QueueValidateResponse queueValidateResponse = jsonbContext.unmarshall(response.getEntity().toString(),
                QueueValidateResponse.class);

        assertThat("Not validated", queueValidateResponse.isValidated(), is(false));
        assertThat("Invalid agency - second element", queueValidateResponse.getMessage(), is("Biblioteksnummeret '654321' tilhører ikke biblioteksgruppen ffu"));
    }

    @Test
    public void testValidateNoRecords() throws Exception {
        final QueueAPI bean = getQueueBean();

        final QueueValidateRequest request = new QueueValidateRequest();
        request.setProvider("the-real-provider");
        request.setQueueType("ffu");
        request.setAgencyText("111111\n"); // Extra line break just for fun
        request.setIncludeDeleted(false);

        Set<String> agenciesStringSet = new HashSet<>();
        agenciesStringSet.add("111111");

        when(bean.rawrepo.getProviders()).thenReturn(Arrays.asList(new QueueProvider("the-real-provider")));
        when(bean.openAgency.getLibrariesByCatalogingTemplateSet("ffu")).thenReturn(agenciesStringSet);
        final Response response = bean.validate(jsonbContext.marshall(request));

        assertThat("Response code 200", response.getStatus(), is(200));

        final QueueValidateResponse queueValidateResponse = jsonbContext.unmarshall(response.getEntity().toString(),
                QueueValidateResponse.class);

        assertThat("Not validated", queueValidateResponse.isValidated(), is(false));
        assertThat("No records found", queueValidateResponse.getMessage(), is("Der blev ikke fundet nogen poster, så intet kan lægges på kø"));
    }

    @Test
    public void testValidateOk() throws Exception {
        final QueueAPI bean = getQueueBean();

        final QueueValidateRequest request = new QueueValidateRequest();
        request.setProvider("the-real-provider");
        request.setQueueType("ffu");
        request.setAgencyText("111111\n"); // Extra line break just for fun
        request.setIncludeDeleted(false);

        final Set<String> agenciesStringSet = new HashSet<>();
        agenciesStringSet.add("111111");

        final Set<Integer> agenciesIntegerSet = new HashSet<>();
        agenciesIntegerSet.add(111111);

        final Set<RecordId> recordIdSet = new HashSet<>();
        recordIdSet.add(new RecordId("11223344", 111111));

        when(bean.rawrepo.getProviders()).thenReturn(Arrays.asList(new QueueProvider("the-real-provider")));
        when(bean.rawrepo.getRecordsForAgencies(agenciesIntegerSet, false)).thenReturn(recordIdSet);
        when(bean.openAgency.getLibrariesByCatalogingTemplateSet("ffu")).thenReturn(agenciesStringSet);
        final Response response = bean.validate(jsonbContext.marshall(request));

        assertThat("Response code 200", response.getStatus(), is(200));

        final QueueValidateResponse queueValidateResponse = jsonbContext.unmarshall(response.getEntity().toString(),
                QueueValidateResponse.class);

        assertThat("Validates", queueValidateResponse.isValidated(), is(true));
        assertThat("Has sessionId", queueValidateResponse.getSessionId(), is(notNullValue()));
        assertThat("Has 1 AgencyAnalysis", queueValidateResponse.getAgencyAnalysisList().size(), is(1));
        assertThat("AgencyList 0 has correct agencyId",
                queueValidateResponse.getAgencyAnalysisList().get(0).getAgencyId(), is(111111));
        assertThat("AgencyList 0 has correct count", queueValidateResponse.getAgencyAnalysisList().get(0).getCount(), is(1));
        assertThat("Has 0 chunks", queueValidateResponse.getChunks(), is(0));

        final String sessionId = queueValidateResponse.getSessionId();

        assertThat("bean has sessionId in cache", bean.jobCache.containsKey(sessionId), is(true));
    }

    @Test
    public void testProcessSessionIdIsNull() throws Exception {
        final QueueAPI bean = getQueueBean();

        final QueueProcessRequest request = new QueueProcessRequest();

        final Response response = bean.process(jsonbContext.marshall(request));

        assertThat("Response code 200", response.getStatus(), is(200));

        final QueueProcessResponse queueProcessResponse =
                jsonbContext.unmarshall(response.getEntity().toString(), QueueProcessResponse.class);

        assertThat("Not validated", queueProcessResponse.isValidated(), is(false));
        assertThat("Missing sessionId", queueProcessResponse.getMessage(), is("Der skal være angivet et sessionId"));
    }

    @Test
    public void testProcessSessionIdNotFound() throws Exception {
        final QueueAPI bean = getQueueBean();

        final QueueProcessRequest request = new QueueProcessRequest();
        request.setSessionId("grydesteg");

        final Response response = bean.process(jsonbContext.marshall(request));

        assertThat("Response code 200", response.getStatus(), is(200));

        final QueueProcessResponse queueProcessResponse =
                jsonbContext.unmarshall(response.getEntity().toString(), QueueProcessResponse.class);

        assertThat("Not validated", queueProcessResponse.isValidated(), is(false));
        assertThat("Invalid sessionId", queueProcessResponse.getMessage(), is("SessionId 'grydesteg' blev ikke fundet"));
    }

    @Test
    public void testProcessChunkIndexTooBig() throws Exception {
        final QueueAPI bean = getQueueBean();

        QueueJob queueJob = new QueueJob();
        queueJob.setAgencyIdList(new HashSet<>());
        queueJob.setRecordIdList(new HashSet<>());

        bean.jobCache.put("the-key", queueJob);

        final QueueProcessRequest request = new QueueProcessRequest();
        request.setSessionId("the-key");
        request.setChunkIndex(1);

        final Response response = bean.process(jsonbContext.marshall(request));

        assertThat("Response code 200", response.getStatus(), is(200));

        final QueueProcessResponse queueProcessResponse =
                jsonbContext.unmarshall(response.getEntity().toString(), QueueProcessResponse.class);

        assertThat("Not validated", queueProcessResponse.isValidated(), is(false));
        assertThat("Chunk index too big", queueProcessResponse.getMessage(), is("Chunk index '1' er for stort"));
    }

    @Test
    public void testProcessChunkIndexTooSmall() throws Exception {
        final QueueAPI bean = getQueueBean();

        QueueJob queueJob = new QueueJob();
        queueJob.setAgencyIdList(new HashSet<>());
        queueJob.setRecordIdList(new HashSet<>());

        bean.jobCache.put("the-key", queueJob);

        final QueueProcessRequest request = new QueueProcessRequest();
        request.setSessionId("the-key");
        request.setChunkIndex(-1);

        final Response response = bean.process(jsonbContext.marshall(request));

        assertThat("Response code 200", response.getStatus(), is(200));

        final QueueProcessResponse queueProcessResponse =
                jsonbContext.unmarshall(response.getEntity().toString(), QueueProcessResponse.class);

        assertThat("Not validated", queueProcessResponse.isValidated(), is(false));
        assertThat("Chunk index is negative", queueProcessResponse.getMessage(), is("Chunk index må ikke være negativt"));
    }

    @Test
    public void testProcessOkFullFlow() throws Exception {
        final QueueAPI bean = getQueueBean();

        final QueueValidateRequest validateRequest = new QueueValidateRequest();
        validateRequest.setProvider("the-real-provider");
        validateRequest.setQueueType("ffu");
        validateRequest.setAgencyText("111111\n");
        validateRequest.setIncludeDeleted(false);

        final Set<String> agenciesStringSet = new HashSet<>();
        agenciesStringSet.add("111111");

        final Set<Integer> agenciesIntegerSet = new HashSet<>();
        agenciesIntegerSet.add(111111);

        final Set<RecordId> recordIdSet = new HashSet<>();
        recordIdSet.add(new RecordId("11223344", 111111));

        when(bean.rawrepo.getProviders()).thenReturn(Arrays.asList(new QueueProvider("the-real-provider")));
        when(bean.rawrepo.getRecordsForAgencies(agenciesIntegerSet, false)).thenReturn(recordIdSet);
        when(bean.openAgency.getLibrariesByCatalogingTemplateSet("ffu")).thenReturn(agenciesStringSet);
        final Response validateResponse = bean.validate(jsonbContext.marshall(validateRequest));

        assertThat("Response code 200 - validate", validateResponse.getStatus(), is(200));

        final QueueValidateResponse queueValidateResponse =
                jsonbContext.unmarshall(validateResponse.getEntity().toString(), QueueValidateResponse.class);

        assertThat("Validates", queueValidateResponse.isValidated(), is(true));
        assertThat("Has sessionId", queueValidateResponse.getSessionId(), is(notNullValue()));

        final String sessionId = queueValidateResponse.getSessionId();

        assertThat("Bean has sessionId in cache", bean.jobCache.containsKey(sessionId), is(true));

        final QueueProcessRequest processRequest = new QueueProcessRequest();
        processRequest.setSessionId(sessionId);
        processRequest.setChunkIndex(0);

        final Response processResponse = bean.process(jsonbContext.marshall(processRequest));

        assertThat("Response code 200 - process", processResponse.getStatus(), is(200));

        final QueueProcessResponse queueProcessResponse =
                jsonbContext.unmarshall(processResponse.getEntity().toString(), QueueProcessResponse.class);

        assertThat("Process validates", queueProcessResponse.isValidated(), is(true));
    }
}
