package org.elasticsearch.plugins;

import org.elasticsearch.action.ActionModule;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.indices.analysis.AnalysisModule;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public abstract class Plugin {

    public Collection<Module> createGuiceModules() {
        return Collections.emptyList();
    }

    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return Collections.emptyList();
    }

    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool, ResourceWatcherService resourceWatcherService, ScriptService scriptService) {
        return Collections.emptyList();
    }

    public Settings additionalSettings() {
        return Settings.Builder.EMPTY_SETTINGS;
    }

    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Collections.emptyList();
    }

    public void onIndexModule(IndexModule indexModule) {
    }

    public List<Setting<?>> getSettings() {
        return Collections.emptyList();
    }

    public List<String> getSettingsFilter() {
        return Collections.emptyList();
    }

    @Deprecated
    public final void onModule(IndexModule indexModule) {
    }

    @Deprecated
    public final void onModule(SettingsModule settingsModule) {
    }

    @Deprecated
    public final void onModule(ScriptModule module) {
    }

    @Deprecated
    public final void onModule(AnalysisModule module) {
    }

    @Deprecated
    public final void onModule(ActionModule module) {
    }

    @Deprecated
    public final void onModule(SearchModule module) {
    }

    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return Collections.emptyList();
    }
}