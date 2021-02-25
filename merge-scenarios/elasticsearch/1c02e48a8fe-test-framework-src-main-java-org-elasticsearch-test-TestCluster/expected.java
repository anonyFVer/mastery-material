package org.elasticsearch.test;

import com.carrotsearch.hppc.ObjectArrayList;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndexTemplateMissingException;
import org.elasticsearch.repositories.RepositoryMissingException;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;
import java.util.Set;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public abstract class TestCluster implements Closeable {

    protected final Logger logger = Loggers.getLogger(getClass());

    private final long seed;

    protected Random random;

    protected double transportClientRatio = 0.0;

    public TestCluster(long seed) {
        this.seed = seed;
    }

    public long seed() {
        return seed;
    }

    public void beforeTest(Random random, double transportClientRatio) throws IOException, InterruptedException {
        assert transportClientRatio >= 0.0 && transportClientRatio <= 1.0;
        logger.debug("Reset test cluster with transport client ratio: [{}]", transportClientRatio);
        this.transportClientRatio = transportClientRatio;
        this.random = new Random(random.nextLong());
    }

    public void wipe(Set<String> excludeTemplates) {
        wipeIndices("_all");
        wipeAllTemplates(excludeTemplates);
        wipeRepositories();
    }

    public void beforeIndexDeletion() {
    }

    public void assertAfterTest() throws IOException {
        ensureEstimatedStats();
    }

    public abstract void afterTest() throws IOException;

    public abstract Client client();

    public abstract int size();

    public abstract int numDataNodes();

    public abstract int numDataAndMasterNodes();

    public abstract InetSocketAddress[] httpAddresses();

    @Override
    public abstract void close() throws IOException;

    public void wipeIndices(String... indices) {
        assert indices != null && indices.length > 0;
        if (size() > 0) {
            try {
                assertAcked(client().admin().indices().prepareDelete(indices));
            } catch (IndexNotFoundException e) {
            } catch (IllegalArgumentException e) {
                if ("_all".equals(indices[0])) {
                    ClusterStateResponse clusterStateResponse = client().admin().cluster().prepareState().execute().actionGet();
                    ObjectArrayList<String> concreteIndices = new ObjectArrayList<>();
                    for (IndexMetaData indexMetaData : clusterStateResponse.getState().metaData()) {
                        concreteIndices.add(indexMetaData.getIndex().getName());
                    }
                    if (!concreteIndices.isEmpty()) {
                        assertAcked(client().admin().indices().prepareDelete(concreteIndices.toArray(String.class)));
                    }
                }
            }
        }
    }

    public void wipeAllTemplates(Set<String> exclude) {
        if (size() > 0) {
            GetIndexTemplatesResponse response = client().admin().indices().prepareGetTemplates().get();
            for (IndexTemplateMetaData indexTemplate : response.getIndexTemplates()) {
                if (exclude.contains(indexTemplate.getName())) {
                    continue;
                }
                try {
                    client().admin().indices().prepareDeleteTemplate(indexTemplate.getName()).execute().actionGet();
                } catch (IndexTemplateMissingException e) {
                }
            }
        }
    }

    public void wipeTemplates(String... templates) {
        if (size() > 0) {
            if (templates.length == 0) {
                templates = new String[] { "*" };
            }
            for (String template : templates) {
                try {
                    client().admin().indices().prepareDeleteTemplate(template).execute().actionGet();
                } catch (IndexTemplateMissingException e) {
                }
            }
        }
    }

    public void wipeRepositories(String... repositories) {
        if (size() > 0) {
            if (repositories.length == 0) {
                repositories = new String[] { "*" };
            }
            for (String repository : repositories) {
                try {
                    client().admin().cluster().prepareDeleteRepository(repository).execute().actionGet();
                } catch (RepositoryMissingException ex) {
                }
            }
        }
    }

    public abstract void ensureEstimatedStats();

    public abstract String getClusterName();

    public abstract Iterable<Client> getClients();
}