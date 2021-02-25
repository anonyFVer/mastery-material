package org.elasticsearch.gateway;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

    private final String prefix;

    private final Pattern stateFilePattern;

    private static final Logger logger = Loggers.getLogger(MetaDataStateFormat.class);

    protected MetaDataStateFormat(String prefix) {
        this.prefix = prefix;
        this.stateFilePattern = Pattern.compile(Pattern.quote(prefix) + "(\\d+)(" + MetaDataStateFormat.STATE_FILE_EXTENSION + ")?");
    }

    private static void deleteFileIfExists(Path stateLocation, Directory directory, String fileName) throws IOException {
        try {
            directory.deleteFile(fileName);
        } catch (FileNotFoundException | NoSuchFileException ignored) {
        }
        logger.trace("cleaned up {}", stateLocation.resolve(fileName));
    }

    private static void deleteFileIgnoreExceptions(Path stateLocation, Directory directory, String fileName) {
        try {
            deleteFileIfExists(stateLocation, directory, fileName);
        } catch (IOException e) {
            logger.trace("clean up failed {}", stateLocation.resolve(fileName));
        }
    }

    private void writeStateToFirstLocation(final T state, Path stateLocation, Directory stateDir, String tmpFileName) throws WriteStateException {
        try {
            deleteFileIfExists(stateLocation, stateDir, tmpFileName);
            try (IndexOutput out = stateDir.createOutput(tmpFileName, IOContext.DEFAULT)) {
                CodecUtil.writeHeader(out, STATE_FILE_CODEC, STATE_FILE_VERSION);
                out.writeInt(FORMAT.index());
                try (XContentBuilder builder = newXContentBuilder(FORMAT, new IndexOutputOutputStream(out) {

                    @Override
                    public void close() {
                    }
                })) {
                    builder.startObject();
                    toXContent(builder, state);
                    builder.endObject();
                }
                CodecUtil.writeFooter(out);
            }
            stateDir.sync(Collections.singleton(tmpFileName));
        } catch (Exception e) {
            throw new WriteStateException(false, "failed to write state to the first location tmp file " + stateLocation.resolve(tmpFileName), e);
        }
    }

    private static void copyStateToExtraLocations(List<Tuple<Path, Directory>> stateDirs, String tmpFileName) throws WriteStateException {
        Directory srcStateDir = stateDirs.get(0).v2();
        for (int i = 1; i < stateDirs.size(); i++) {
            Tuple<Path, Directory> extraStatePathAndDir = stateDirs.get(i);
            Path extraStateLocation = extraStatePathAndDir.v1();
            Directory extraStateDir = extraStatePathAndDir.v2();
            try {
                deleteFileIfExists(extraStateLocation, extraStateDir, tmpFileName);
                extraStateDir.copyFrom(srcStateDir, tmpFileName, tmpFileName, IOContext.DEFAULT);
                extraStateDir.sync(Collections.singleton(tmpFileName));
            } catch (Exception e) {
                throw new WriteStateException(false, "failed to copy tmp state file to extra location " + extraStateLocation, e);
            }
        }
    }

    private static void performRenames(String tmpFileName, String fileName, final List<Tuple<Path, Directory>> stateDirectories) throws WriteStateException {
        Directory firstStateDirectory = stateDirectories.get(0).v2();
        try {
            firstStateDirectory.rename(tmpFileName, fileName);
        } catch (IOException e) {
            throw new WriteStateException(false, "failed to rename tmp file to final name in the first state location " + stateDirectories.get(0).v1().resolve(tmpFileName), e);
        }
        for (int i = 1; i < stateDirectories.size(); i++) {
            Directory extraStateDirectory = stateDirectories.get(i).v2();
            try {
                extraStateDirectory.rename(tmpFileName, fileName);
            } catch (IOException e) {
                throw new WriteStateException(true, "failed to rename tmp file to final name in extra state location " + stateDirectories.get(i).v1().resolve(tmpFileName), e);
            }
        }
    }

    private static void performStateDirectoriesFsync(List<Tuple<Path, Directory>> stateDirectories) throws WriteStateException {
        for (int i = 0; i < stateDirectories.size(); i++) {
            try {
                stateDirectories.get(i).v2().syncMetaData();
            } catch (IOException e) {
                throw new WriteStateException(true, "meta data directory fsync has failed " + stateDirectories.get(i).v1(), e);
            }
        }
    }

    public final void write(final T state, final Path... locations) throws WriteStateException {
        if (locations == null) {
            throw new IllegalArgumentException("Locations must not be null");
        }
        if (locations.length <= 0) {
            throw new IllegalArgumentException("One or more locations required");
        }
        long maxStateId;
        try {
            maxStateId = findMaxStateId(prefix, locations) + 1;
        } catch (Exception e) {
            throw new WriteStateException(false, "exception during looking up max state id", e);
        }
        assert maxStateId >= 0 : "maxStateId must be positive but was: [" + maxStateId + "]";
        final String fileName = prefix + maxStateId + STATE_FILE_EXTENSION;
        final String tmpFileName = fileName + ".tmp";
        List<Tuple<Path, Directory>> directories = new ArrayList<>();
        try {
            for (Path location : locations) {
                Path stateLocation = location.resolve(STATE_DIR_NAME);
                try {
                    directories.add(new Tuple<>(location, newDirectory(stateLocation)));
                } catch (IOException e) {
                    throw new WriteStateException(false, "failed to open state directory " + stateLocation, e);
                }
            }
            writeStateToFirstLocation(state, directories.get(0).v1(), directories.get(0).v2(), tmpFileName);
            copyStateToExtraLocations(directories, tmpFileName);
            performRenames(tmpFileName, fileName, directories);
            performStateDirectoriesFsync(directories);
        } finally {
            for (Tuple<Path, Directory> pathAndDirectory : directories) {
                deleteFileIgnoreExceptions(pathAndDirectory.v1(), pathAndDirectory.v2(), tmpFileName);
                IOUtils.closeWhileHandlingException(pathAndDirectory.v2());
            }
        }
        cleanupOldFiles(fileName, locations);
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

    private void cleanupOldFiles(final String currentStateFile, Path[] locations) {
        for (Path location : locations) {
            logger.trace("cleanupOldFiles: cleaning up {}", location);
            Path stateLocation = location.resolve(STATE_DIR_NAME);
            try (Directory stateDir = newDirectory(stateLocation)) {
                for (String file : stateDir.listAll()) {
                    if (file.startsWith(prefix) && file.equals(currentStateFile) == false) {
                        deleteFileIgnoreExceptions(stateLocation, stateDir, file);
                    }
                }
            } catch (Exception e) {
                logger.trace("clean up failed for state location {}", stateLocation);
            }
        }
    }

    private long findMaxStateId(final String prefix, Path... locations) throws IOException {
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

    private List<Path> findStateFilesByGeneration(final long generation, Path... locations) {
        List<Path> files = new ArrayList<>();
        if (generation == -1) {
            return files;
        }
        final String fileName = prefix + generation + STATE_FILE_EXTENSION;
        for (Path dataLocation : locations) {
            final Path stateFilePath = dataLocation.resolve(STATE_DIR_NAME).resolve(fileName);
            if (Files.exists(stateFilePath)) {
                logger.trace("found state file: {}", stateFilePath);
                files.add(stateFilePath);
            }
        }
        return files;
    }

    public T loadLatestState(Logger logger, NamedXContentRegistry namedXContentRegistry, Path... dataLocations) throws IOException {
        long maxStateId = findMaxStateId(prefix, dataLocations);
        List<Path> stateFiles = findStateFilesByGeneration(maxStateId, dataLocations);
        if (maxStateId > -1 && stateFiles.isEmpty()) {
            throw new IllegalStateException("unable to find state files with state id " + maxStateId + " returned by findMaxStateId function, in data folders [" + Arrays.stream(dataLocations).map(Path::toAbsolutePath).map(Object::toString).collect(Collectors.joining(", ")) + "], concurrent writes?");
        }
        final List<Throwable> exceptions = new ArrayList<>();
        for (Path stateFile : stateFiles) {
            try {
                T state = read(namedXContentRegistry, stateFile);
                logger.trace("state id [{}] read from [{}]", maxStateId, stateFile.getFileName());
                return state;
            } catch (Exception e) {
                exceptions.add(new IOException("failed to read " + stateFile.toAbsolutePath(), e));
                logger.debug(() -> new ParameterizedMessage("{}: failed to read [{}], ignoring...", stateFile.toAbsolutePath(), prefix), e);
            }
        }
        ExceptionsHelper.maybeThrowRuntimeAndSuppress(exceptions);
        if (stateFiles.size() > 0) {
            throw new IllegalStateException("Could not find a state file to recover from among " + stateFiles.stream().map(Path::toAbsolutePath).map(Object::toString).collect(Collectors.joining(", ")));
        }
        return null;
    }

    public static void deleteMetaState(Path... dataLocations) throws IOException {
        Path[] stateDirectories = new Path[dataLocations.length];
        for (int i = 0; i < dataLocations.length; i++) {
            stateDirectories[i] = dataLocations[i].resolve(STATE_DIR_NAME);
        }
        IOUtils.rm(stateDirectories);
    }
}