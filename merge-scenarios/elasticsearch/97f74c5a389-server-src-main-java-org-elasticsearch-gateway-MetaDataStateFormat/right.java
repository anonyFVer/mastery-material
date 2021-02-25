package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.OutputStreamIndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.IOUtils;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public abstract class MetaDataStateFormat<T> {

    public static final XContentType FORMAT = XContentType.SMILE;

    public static final String STATE_DIR_NAME = "_state";

    public static final String STATE_FILE_EXTENSION = ".st";

    private static final String STATE_FILE_CODEC = "state";

    private static final int MIN_COMPATIBLE_STATE_FILE_VERSION = 1;

    private static final int STATE_FILE_VERSION = 1;

    private static final int BUFFER_SIZE = 4096;

    private final String prefix;

    private final Pattern stateFilePattern;

    private static final Logger logger = LogManager.getLogger(MetaDataStateFormat.class);

    protected MetaDataStateFormat(String prefix) {
        this.prefix = prefix;
        this.stateFilePattern = Pattern.compile(Pattern.quote(prefix) + "(\\d+)(" + MetaDataStateFormat.STATE_FILE_EXTENSION + ")?");
    }

    public final void write(final T state, final Path... locations) throws IOException {
        if (locations == null) {
            throw new IllegalArgumentException("Locations must not be null");
        }
        if (locations.length <= 0) {
            throw new IllegalArgumentException("One or more locations required");
        }
        final long maxStateId = findMaxStateId(prefix, locations) + 1;
        assert maxStateId >= 0 : "maxStateId must be positive but was: [" + maxStateId + "]";
        final String fileName = prefix + maxStateId + STATE_FILE_EXTENSION;
        Path stateLocation = locations[0].resolve(STATE_DIR_NAME);
        Files.createDirectories(stateLocation);
        final Path tmpStatePath = stateLocation.resolve(fileName + ".tmp");
        final Path finalStatePath = stateLocation.resolve(fileName);
        try {
            final String resourceDesc = "MetaDataStateFormat.write(path=\"" + tmpStatePath + "\")";
            try (OutputStreamIndexOutput out = new OutputStreamIndexOutput(resourceDesc, fileName, Files.newOutputStream(tmpStatePath), BUFFER_SIZE)) {
                CodecUtil.writeHeader(out, STATE_FILE_CODEC, STATE_FILE_VERSION);
                out.writeInt(FORMAT.index());
                try (XContentBuilder builder = newXContentBuilder(FORMAT, new IndexOutputOutputStream(out) {

                    @Override
                    public void close() throws IOException {
                    }
                })) {
                    builder.startObject();
                    {
                        toXContent(builder, state);
                    }
                    builder.endObject();
                }
                CodecUtil.writeFooter(out);
            }
            IOUtils.fsync(tmpStatePath, false);
            Files.move(tmpStatePath, finalStatePath, StandardCopyOption.ATOMIC_MOVE);
            IOUtils.fsync(stateLocation, true);
            logger.trace("written state to {}", finalStatePath);
            for (int i = 1; i < locations.length; i++) {
                stateLocation = locations[i].resolve(STATE_DIR_NAME);
                Files.createDirectories(stateLocation);
                Path tmpPath = stateLocation.resolve(fileName + ".tmp");
                Path finalPath = stateLocation.resolve(fileName);
                try {
                    Files.copy(finalStatePath, tmpPath);
                    IOUtils.fsync(tmpPath, false);
                    Files.move(tmpPath, finalPath, StandardCopyOption.ATOMIC_MOVE);
                    IOUtils.fsync(stateLocation, true);
                    logger.trace("copied state to {}", finalPath);
                } finally {
                    Files.deleteIfExists(tmpPath);
                    logger.trace("cleaned up {}", tmpPath);
                }
            }
        } finally {
            Files.deleteIfExists(tmpStatePath);
            logger.trace("cleaned up {}", tmpStatePath);
        }
        cleanupOldFiles(prefix, fileName, locations);
    }

    protected XContentBuilder newXContentBuilder(XContentType type, OutputStream stream) throws IOException {
        return XContentFactory.contentBuilder(type, stream);
    }

    public abstract void toXContent(XContentBuilder builder, T state) throws IOException;

    public abstract T fromXContent(XContentParser parser) throws IOException;

    public final T read(NamedXContentRegistry namedXContentRegistry, Path file) throws IOException {
        try (Directory dir = newDirectory(file.getParent())) {
            try (IndexInput indexInput = dir.openInput(file.getFileName().toString(), IOContext.DEFAULT)) {
                CodecUtil.checksumEntireFile(indexInput);
                CodecUtil.checkHeader(indexInput, STATE_FILE_CODEC, MIN_COMPATIBLE_STATE_FILE_VERSION, STATE_FILE_VERSION);
                final XContentType xContentType = XContentType.values()[indexInput.readInt()];
                if (xContentType != FORMAT) {
                    throw new IllegalStateException("expected state in " + file + " to be " + FORMAT + " format but was " + xContentType);
                }
                long filePointer = indexInput.getFilePointer();
                long contentSize = indexInput.length() - CodecUtil.footerLength() - filePointer;
                try (IndexInput slice = indexInput.slice("state_xcontent", filePointer, contentSize)) {
                    try (XContentParser parser = XContentFactory.xContent(FORMAT).createParser(namedXContentRegistry, LoggingDeprecationHandler.INSTANCE, new InputStreamIndexInput(slice, contentSize))) {
                        return fromXContent(parser);
                    }
                }
            } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
                throw new CorruptStateException(ex);
            }
        }
    }

    protected Directory newDirectory(Path dir) throws IOException {
        return new SimpleFSDirectory(dir);
    }

    private void cleanupOldFiles(final String prefix, final String currentStateFile, Path[] locations) throws IOException {
        final DirectoryStream.Filter<Path> filter = entry -> {
            final String entryFileName = entry.getFileName().toString();
            return Files.isRegularFile(entry) && entryFileName.startsWith(prefix) && currentStateFile.equals(entryFileName) == false;
        };
        for (Path dataLocation : locations) {
            logger.trace("cleanupOldFiles: cleaning up {}", dataLocation);
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(dataLocation.resolve(STATE_DIR_NAME), filter)) {
                for (Path stateFile : stream) {
                    Files.deleteIfExists(stateFile);
                    logger.trace("cleanupOldFiles: cleaned up {}", stateFile);
                }
            }
        }
    }

    long findMaxStateId(final String prefix, Path... locations) throws IOException {
        long maxId = -1;
        for (Path dataLocation : locations) {
            final Path resolve = dataLocation.resolve(STATE_DIR_NAME);
            if (Files.exists(resolve)) {
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(resolve, prefix + "*")) {
                    for (Path stateFile : stream) {
                        final Matcher matcher = stateFilePattern.matcher(stateFile.getFileName().toString());
                        if (matcher.matches()) {
                            final long id = Long.parseLong(matcher.group(1));
                            maxId = Math.max(maxId, id);
                        }
                    }
                }
            }
        }
        return maxId;
    }

    public T loadLatestState(Logger logger, NamedXContentRegistry namedXContentRegistry, Path... dataLocations) throws IOException {
        List<PathAndStateId> files = new ArrayList<>();
        long maxStateId = -1;
        if (dataLocations != null) {
            for (Path dataLocation : dataLocations) {
                final Path stateDir = dataLocation.resolve(STATE_DIR_NAME);
                try (DirectoryStream<Path> paths = Files.newDirectoryStream(stateDir)) {
                    for (Path stateFile : paths) {
                        final Matcher matcher = stateFilePattern.matcher(stateFile.getFileName().toString());
                        if (matcher.matches()) {
                            final long stateId = Long.parseLong(matcher.group(1));
                            maxStateId = Math.max(maxStateId, stateId);
                            PathAndStateId pav = new PathAndStateId(stateFile, stateId);
                            logger.trace("found state file: {}", pav);
                            files.add(pav);
                        }
                    }
                } catch (NoSuchFileException | FileNotFoundException ex) {
                }
            }
        }
        long finalMaxStateId = maxStateId;
        Collection<PathAndStateId> pathAndStateIds = files.stream().filter(pathAndStateId -> pathAndStateId.id == finalMaxStateId).collect(Collectors.toCollection(ArrayList::new));
        final List<Throwable> exceptions = new ArrayList<>();
        for (PathAndStateId pathAndStateId : pathAndStateIds) {
            try {
                T state = read(namedXContentRegistry, pathAndStateId.file);
                logger.trace("state id [{}] read from [{}]", pathAndStateId.id, pathAndStateId.file.getFileName());
                return state;
            } catch (Exception e) {
                exceptions.add(new IOException("failed to read " + pathAndStateId.toString(), e));
                logger.debug(() -> new ParameterizedMessage("{}: failed to read [{}], ignoring...", pathAndStateId.file.toAbsolutePath(), prefix), e);
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
        if (files.size() > 0) {
            throw new IllegalStateException("Could not find a state file to recover from among " + files);
        }
        return null;
    }

    private static class PathAndStateId {

        final Path file;

        final long id;

        private PathAndStateId(Path file, long id) {
            this.file = file;
            this.id = id;
        }

        @Override
        public String toString() {
            return "[id:" + id + ", file:" + file.toAbsolutePath() + "]";
        }
    }

    public static void deleteMetaState(Path... dataLocations) throws IOException {
        Path[] stateDirectories = new Path[dataLocations.length];
        for (int i = 0; i < dataLocations.length; i++) {
            stateDirectories[i] = dataLocations[i].resolve(STATE_DIR_NAME);
        }
        IOUtils.rm(stateDirectories);
    }
}