package fr.milekat.utils.storage.adapter.elasticsearch.features;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.mapping.DynamicMapping;
import fr.milekat.utils.storage.StorageLoader;
import fr.milekat.utils.storage.adapter.elasticsearch.utils.Mapping;
import fr.milekat.utils.storage.exceptions.StorageLoadException;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unused")
public class Index {
    private final ElasticsearchClient client;
    private final String indexName;
    private final String numberOfReplicas;
    private final Map<String, Class<?>> fields = new HashMap<>();
    private final Map<String, Class<?>> tags = new HashMap<>();
    private final String tagsFieldName;
    private final boolean strictMapping;

    public Index(@NotNull ElasticsearchClient client,
                 @NotNull String indexName,
                 @NotNull String numberOfReplicas,
                 @NotNull Map<String, Class<?>> fields,
                 boolean strictMapping) throws StorageLoadException {
        this.client = client;
        this.indexName = indexName;
        this.numberOfReplicas = numberOfReplicas;
        this.fields.putAll(fields);
        this.tagsFieldName = null;
        this.strictMapping = strictMapping;
        load();
    }

    public Index(@NotNull ElasticsearchClient client,
                 @NotNull String indexName,
                 @NotNull String numberOfReplicas,
                 @NotNull Map<String, Class<?>> fields,
                 @NotNull Map<String, Class<?>> tags,
                 @NotNull String tagsFieldName,
                 boolean strictMapping) throws StorageLoadException {
        this.client = client;
        this.indexName = indexName;
        this.numberOfReplicas = numberOfReplicas;
        this.fields.putAll(fields);
        this.tags.putAll(tags);
        this.tagsFieldName = tagsFieldName;
        this.strictMapping = strictMapping;
        load();
    }

    public void load() throws StorageLoadException {
        //  Check if index exist, otherwise create it
        if (!isIndexExist()) {
            StorageLoader.getStorageLogger().debug("Index '" + indexName + "' not found, creating...");
            createIndex();
        }
        StorageLoader.getStorageLogger().debug("Index '" + indexName + "' loaded !");
    }

    public boolean isIndexExist() throws StorageLoadException {
        try {
            StorageLoader.getStorageLogger().debug("Check if index '" + indexName + "' is present...");
            return client.indices().exists(e -> e.index(indexName)).value();
        } catch (ElasticsearchException | IOException exception) {
            StorageLoader.getStorageLogger().warning("ElasticSearch client error.");
            StorageLoader.getStorageLogger().stack(exception.getStackTrace());
            throw new StorageLoadException("Index check error: " + exception.getMessage());
        }
    }

    public void createIndex() throws StorageLoadException {
        try {
            StorageLoader.getStorageLogger().debug("Creating index '" + indexName + "'...");
            client.indices().create(c -> c.index(indexName)
                    .mappings(m -> {
                        m.properties(Mapping.getMapping(fields, tags, tagsFieldName));
                        if (strictMapping) m.dynamic(DynamicMapping.Strict);
                        return m;
                    })
                    .settings(s -> s.numberOfReplicas(numberOfReplicas))
            );
            StorageLoader.getStorageLogger().debug("Index '" + indexName + "' created !");
        } catch (ElasticsearchException | IOException exception) {
            StorageLoader.getStorageLogger().warning("ElasticSearch client error.");
            StorageLoader.getStorageLogger().stack(exception.getStackTrace());
            throw new StorageLoadException("Index create error: " + exception.getMessage());
        }
    }
}
