package org.elasticsearch.indices;

import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.cluster.metadata.MetaDataIndexUpgradeService;
import org.elasticsearch.common.geo.ShapesAvailability;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry.Entry;
import org.elasticsearch.index.NodeServicesProvider;
import org.elasticsearch.index.mapper.AllFieldMapper;
import org.elasticsearch.index.mapper.BinaryFieldMapper;
import org.elasticsearch.index.mapper.BooleanFieldMapper;
import org.elasticsearch.index.mapper.CompletionFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.IpFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.LatLonPointFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.index.mapper.TextFieldMapper;
import org.elasticsearch.index.mapper.TokenCountFieldMapper;
import org.elasticsearch.index.mapper.ScaledFloatFieldMapper;
import org.elasticsearch.index.mapper.StringFieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.IndexFieldMapper;
import org.elasticsearch.index.mapper.ParentFieldMapper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.TimestampFieldMapper;
import org.elasticsearch.index.mapper.TTLFieldMapper;
import org.elasticsearch.index.mapper.TypeFieldMapper;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.mapper.VersionFieldMapper;
import org.elasticsearch.indices.cluster.IndicesClusterStateService;
import org.elasticsearch.indices.flush.SyncedFlushService;
import org.elasticsearch.indices.mapper.MapperRegistry;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.PeerRecoverySourceService;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.store.IndicesStore;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.indices.ttl.IndicesTTLService;
import org.elasticsearch.plugins.MapperPlugin;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class IndicesModule extends AbstractModule {

    private final Map<String, Mapper.TypeParser> mapperParsers;

    private final Map<String, MetadataFieldMapper.TypeParser> metadataMapperParsers;

    private final MapperRegistry mapperRegistry;

    private final List<Entry> namedWritables = new ArrayList<>();

    public IndicesModule(List<MapperPlugin> mapperPlugins) {
        this.mapperParsers = getMappers(mapperPlugins);
        this.metadataMapperParsers = getMetadataMappers(mapperPlugins);
        this.mapperRegistry = new MapperRegistry(mapperParsers, metadataMapperParsers);
        registerBuiltinWritables();
    }

    private void registerBuiltinWritables() {
        namedWritables.add(new Entry(Condition.class, MaxAgeCondition.NAME, MaxAgeCondition::new));
        namedWritables.add(new Entry(Condition.class, MaxDocsCondition.NAME, MaxDocsCondition::new));
    }

    public List<Entry> getNamedWriteables() {
        return namedWritables;
    }

    private Map<String, Mapper.TypeParser> getMappers(List<MapperPlugin> mapperPlugins) {
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();
        for (NumberFieldMapper.NumberType type : NumberFieldMapper.NumberType.values()) {
            mappers.put(type.typeName(), new NumberFieldMapper.TypeParser(type));
        }
        mappers.put(BooleanFieldMapper.CONTENT_TYPE, new BooleanFieldMapper.TypeParser());
        mappers.put(BinaryFieldMapper.CONTENT_TYPE, new BinaryFieldMapper.TypeParser());
        mappers.put(DateFieldMapper.CONTENT_TYPE, new DateFieldMapper.TypeParser());
        mappers.put(IpFieldMapper.CONTENT_TYPE, new IpFieldMapper.TypeParser());
        mappers.put(ScaledFloatFieldMapper.CONTENT_TYPE, new ScaledFloatFieldMapper.TypeParser());
        mappers.put(StringFieldMapper.CONTENT_TYPE, new StringFieldMapper.TypeParser());
        mappers.put(TextFieldMapper.CONTENT_TYPE, new TextFieldMapper.TypeParser());
        mappers.put(KeywordFieldMapper.CONTENT_TYPE, new KeywordFieldMapper.TypeParser());
        mappers.put(TokenCountFieldMapper.CONTENT_TYPE, new TokenCountFieldMapper.TypeParser());
        mappers.put(ObjectMapper.CONTENT_TYPE, new ObjectMapper.TypeParser());
        mappers.put(ObjectMapper.NESTED_CONTENT_TYPE, new ObjectMapper.TypeParser());
        mappers.put(CompletionFieldMapper.CONTENT_TYPE, new CompletionFieldMapper.TypeParser());
        mappers.put(GeoPointFieldMapper.CONTENT_TYPE, new GeoPointFieldMapper.TypeParser());
        mappers.put(LatLonPointFieldMapper.CONTENT_TYPE, new LatLonPointFieldMapper.TypeParser());
        if (ShapesAvailability.JTS_AVAILABLE && ShapesAvailability.SPATIAL4J_AVAILABLE) {
            mappers.put(GeoShapeFieldMapper.CONTENT_TYPE, new GeoShapeFieldMapper.TypeParser());
        }
        for (MapperPlugin mapperPlugin : mapperPlugins) {
            for (Map.Entry<String, Mapper.TypeParser> entry : mapperPlugin.getMappers().entrySet()) {
                if (mappers.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Mapper [" + entry.getKey() + "] is already registered");
                }
            }
        }
        return Collections.unmodifiableMap(mappers);
    }

    private Map<String, MetadataFieldMapper.TypeParser> getMetadataMappers(List<MapperPlugin> mapperPlugins) {
        Map<String, MetadataFieldMapper.TypeParser> metadataMappers = new LinkedHashMap<>();
        metadataMappers.put(UidFieldMapper.NAME, new UidFieldMapper.TypeParser());
        metadataMappers.put(IdFieldMapper.NAME, new IdFieldMapper.TypeParser());
        metadataMappers.put(RoutingFieldMapper.NAME, new RoutingFieldMapper.TypeParser());
        metadataMappers.put(IndexFieldMapper.NAME, new IndexFieldMapper.TypeParser());
        metadataMappers.put(SourceFieldMapper.NAME, new SourceFieldMapper.TypeParser());
        metadataMappers.put(TypeFieldMapper.NAME, new TypeFieldMapper.TypeParser());
        metadataMappers.put(AllFieldMapper.NAME, new AllFieldMapper.TypeParser());
        metadataMappers.put(TimestampFieldMapper.NAME, new TimestampFieldMapper.TypeParser());
        metadataMappers.put(TTLFieldMapper.NAME, new TTLFieldMapper.TypeParser());
        metadataMappers.put(VersionFieldMapper.NAME, new VersionFieldMapper.TypeParser());
        metadataMappers.put(ParentFieldMapper.NAME, new ParentFieldMapper.TypeParser());
        for (MapperPlugin mapperPlugin : mapperPlugins) {
            for (Map.Entry<String, MetadataFieldMapper.TypeParser> entry : mapperPlugin.getMetadataMappers().entrySet()) {
                if (entry.getKey().equals(FieldNamesFieldMapper.NAME)) {
                    throw new IllegalArgumentException("Plugin cannot contain metadata mapper [" + FieldNamesFieldMapper.NAME + "]");
                }
                if (metadataMappers.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("MetadataFieldMapper [" + entry.getKey() + "] is already registered");
                }
            }
        }
        metadataMappers.put(FieldNamesFieldMapper.NAME, new FieldNamesFieldMapper.TypeParser());
        return Collections.unmodifiableMap(metadataMappers);
    }

    @Override
    protected void configure() {
        bindMapperExtension();
        bind(RecoverySettings.class).asEagerSingleton();
        bind(PeerRecoveryTargetService.class).asEagerSingleton();
        bind(PeerRecoverySourceService.class).asEagerSingleton();
        bind(IndicesStore.class).asEagerSingleton();
        bind(IndicesClusterStateService.class).asEagerSingleton();
        bind(SyncedFlushService.class).asEagerSingleton();
        bind(TransportNodesListShardStoreMetaData.class).asEagerSingleton();
        bind(IndicesTTLService.class).asEagerSingleton();
        bind(UpdateHelper.class).asEagerSingleton();
        bind(MetaDataIndexUpgradeService.class).asEagerSingleton();
        bind(NodeServicesProvider.class).asEagerSingleton();
    }

    public MapperRegistry getMapperRegistry() {
        return mapperRegistry;
    }

    protected void bindMapperExtension() {
        bind(MapperRegistry.class).toInstance(getMapperRegistry());
    }
}