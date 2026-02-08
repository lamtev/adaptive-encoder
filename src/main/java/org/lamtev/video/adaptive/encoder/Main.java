import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.commons.lang3.time.DurationFormatUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

void main(String... args) throws IOException, InterruptedException, ExecutionException {
    long start = System.nanoTime();

    CommandLineArgs commandLineArgs = parseCommandLineArgs(args);

    Input input = new Input(commandLineArgs.input, commandLineArgs.fromTime, commandLineArgs.duration);
    String output = commandLineArgs.output;
    String avSceneChange = commandLineArgs.avSceneChange;
    String ffmpegPath = commandLineArgs.ffmpegPath;
    int targetMeanVmaf = commandLineArgs.targetVmaf;
    int targetMinVmaf = commandLineArgs.targetMinVmaf;
    int targetGopInSeconds = commandLineArgs.targetGopInSeconds;
    EncoderName encoder = commandLineArgs.encoder;
    String preset = commandLineArgs.preset;
    DeinterlaceAlgorithm deinterlace = commandLineArgs.deinterlaceAlgorithm;
    DeinterlaceMode deinterlaceMode = commandLineArgs.deinterlaceMode;
    String deinterlaceModelPath = commandLineArgs.deinterlaceModelPath;
    DeinterlaceParams deinterlaceParams = new DeinterlaceParams(deinterlace, deinterlaceMode, deinterlaceModelPath);

    EncodingParams encodingParams = new EncodingParams(encoder, preset, deinterlaceParams, commandLineArgs.denoise, commandLineArgs.pixelFormat);
    int parallelism = commandLineArgs.parallelism;

    ProbeResult probeResult = ffprobe(input, ffmpegPath);
    IO.println("Probe result: %s".formatted(probeResult));
    SceneChanges sceneChanges = detectSceneChanges(avSceneChange, input, probeResult, ffmpegPath);
    IO.println("Scene changes: %s".formatted(sceneChanges));

    Rational frameRate = probeResult.frameRate();
    if (deinterlaceParams.doublesFrameRate()) {
        frameRate = new Rational(frameRate.numerator() * 2, frameRate.denominator());
    }

    int targetGopInFrames = Math.toIntExact(frameRate.multiply(targetGopInSeconds));
    List<Range> scenes = makeScenes(sceneChanges, deinterlaceParams, frameRate, input.from());
    IO.println("Scenes: %s".formatted(scenes));

    List<Range> gops = makeGops(scenes, targetGopInFrames);
    IO.println("GOPs: %s".formatted(gops));

    List<EncodingResult> encodedGops = encode(gops, input.file(), probeResult, frameRate, encodingParams, parallelism, targetMeanVmaf, targetMinVmaf, ffmpegPath);

    String concatenated = concat(encodedGops, encoder, targetMeanVmaf, ffmpegPath);

    Duration processingDuration = Duration.ofNanos(System.nanoTime() - start);

    mergeVideoAndAudio(concatenated, input, output, ffmpegPath);

    makeReport(encodedGops, frameRate, output, processingDuration);

    cleanup(gops, encodingParams, targetMeanVmaf);
}

private CommandLineArgs parseCommandLineArgs(String[] args) {
    CommandLineArgs commandLineArgs = new CommandLineArgs();

    JCommander jCommander = JCommander.newBuilder()
        .addObject(commandLineArgs)
        .build();
    try {
        jCommander.parse(args);
    } catch (ParameterException e) {
        StringBuilder msg = new StringBuilder();

        msg.append(applicationInfo()).append(System.lineSeparator())
            .append(e.getMessage()).append(System.lineSeparator());
        jCommander.usage(msg);
        System.err.println(msg);
        Runtime.getRuntime().halt(-1);
    }

    if (commandLineArgs.help) {
        IO.println(applicationInfo());
        jCommander.usage();
        Runtime.getRuntime().halt(0);
    }

    if ("def".equals(commandLineArgs.preset)) {
        commandLineArgs.preset = defaultPresets.get(commandLineArgs.encoder);
    }

    return commandLineArgs;
}

ApplicationInfo applicationInfo() {
    Package aPackage = getClass().getPackage();
    if (aPackage.getName().isEmpty()) {
        Properties manifest = new Properties();
        try (InputStream manifestStream = getClass().getClassLoader().getResourceAsStream("META-INF/MANIFEST.MF")) {
            manifest.load(manifestStream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new ApplicationInfo(
            manifest.getProperty("Implementation-Title"),
            manifest.getProperty("Implementation-Version"),
            manifest.getProperty("Implementation-Vendor")
        );
    }
    return new ApplicationInfo(
        aPackage.getImplementationTitle(),
        aPackage.getImplementationVersion(),
        aPackage.getImplementationVendor()
    );
}

String ffBinary(String util, String path) {
    return StringUtils.isBlank(path) ? util : path + "/" + util;
}

ProbeResult ffprobe(Input input, String ffmpegPath) throws IOException {
    Process ffprobe = new ProcessBuilder()
        .command(command(
            ffBinary("ffprobe", ffmpegPath),
            readIntervals(input),
            "-select_streams", "v",
            "-show_streams", "-count_packets",
            "-print_format", "json", input.file()
        ))
        .redirectError(ProcessBuilder.Redirect.DISCARD)
        .start();

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Out(List<Stream> streams) {
        @JsonIgnoreProperties(ignoreUnknown = true)
        record Stream(Integer nb_frames, Integer nb_read_packets, String avg_frame_rate, String r_frame_rate,
                      double start_time, double duration) {
        }
    }

    try (Reader stdout = ffprobe.inputReader()) {
        Out res = new ObjectMapper().readValue(stdout, Out.class);
        Out.Stream stream = res.streams().getFirst();
        return new ProbeResult(
            Optional.ofNullable(stream.nb_read_packets())
                .orElse(stream.nb_frames()),
            rationalFromString(
                Optional.ofNullable(stream.avg_frame_rate())
                    .orElse(stream.r_frame_rate())
            ),
            stream.start_time(),
            stream.duration()
        );
    }
}

List<String> readIntervals(Input input) {
    if (input.from() == null && input.duration() == null) {
        return List.of();
    }

    if (input.duration() == null) {
        return List.of("-read_intervals", "%s%%".formatted(durationToString(input.from())));
    }

    if (input.from() == null) {
        return List.of("-read_intervals", "%%+%s".formatted(durationToString(input.duration())));
    }

    return List.of("-read_intervals", "%s%%+%s"
        .formatted(durationToString(input.from()), durationToString(input.duration())));
}

String durationToString(Duration duration) {
    if (duration == null) {
        return "";
    }
    return DurationFormatUtils.formatDurationHMS(duration.toMillis());
}

void validate(ProbeResult probeResult, SceneChanges sceneChanges) {
    if (probeResult.frameCount() != sceneChanges.frameCount()) {
//        IO.println("ffprobe and av-scenechange result mismatch: ffprobe.frameCount=%d != av-scenechange.frameCount=%d".formatted(probeResult.frameCount(), sceneChanges.frameCount()));
        throw new IllegalStateException("ffprobe and av-scenechange result mismatch: ffprobe.frameCount=%d != av-scenechange.frameCount=%d".formatted(probeResult.frameCount(), sceneChanges.frameCount()));
    }
}

SceneChanges detectSceneChanges(String avSceneChange, Input input, ProbeResult probeResult, String ffmpegPath) throws IOException, InterruptedException {
    String scenesJsonFile = "scenes-%s.json".formatted(UUID.randomUUID());

    // We read file using ffmpeg directly because av-scenechange's demuxing/decoding are buggy.
    // Sometimes av-scenechange demuxes/decodes less frames than input coded stream contains.
    List<Process> pipeline = ProcessBuilder.startPipeline(List.of(
        new ProcessBuilder()
            .command(command(
                ffBinary("ffmpeg", ffmpegPath),
                seek(input.from()),
                "-i", input.file(),
                duration(input.duration()),
                input.from() == null && input.duration() == null
                    ? List.of("-frames:v", probeResult.frameCount())
                    : List.of(),
                "-an",
                "-f", "yuv4mpegpipe", "-"
            )),
        new ProcessBuilder()
            .command(avSceneChange, "-o", scenesJsonFile, "-")
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
    ));

    Process sceneCut = pipeline.getLast();

    int sceneCutExit = sceneCut.waitFor();
    if (sceneCutExit != 0) {
        throw new IllegalStateException("Scenecut failed: %d. %s".formatted(sceneCutExit, new String(sceneCut.getErrorStream().readAllBytes(), StandardCharsets.UTF_8)));
    }

    SceneChanges sceneChanges = new ObjectMapper().readValue(new File(scenesJsonFile), SceneChanges.class);
    if (sceneChanges.sceneChanges() == null || sceneChanges.sceneChanges().isEmpty()) {
        IO.println("No scene detected");
        throw new IllegalStateException("No scene detected");
    }

    validate(probeResult, sceneChanges);

    Files.delete(Path.of(scenesJsonFile));

    return new SceneChanges(sceneChanges.sceneChanges(), probeResult.frameCount());
}

List<String> seek(Duration seek) {
    if (seek == null) {
        return List.of();
    }
    return List.of("-ss", DurationFormatUtils.formatDurationHMS(seek.toMillis()));
}

List<String> duration(Duration duration) {
    if (duration == null) {
        return List.of();
    }
    return List.of("-t", DurationFormatUtils.formatDurationHMS(duration.toMillis()));
}

List<Range> makeScenes(SceneChanges sceneChanges, DeinterlaceParams deinterlace, Rational frameRate, Duration from) {
    int offset = Math.toIntExact(from == null ? 0 : frameRate.multiply(from.toMillis()) / 1000);
    int factor = deinterlace.doublesFrameRate() ? 2 : 1;
    return Stream.concat(
            IntStream.range(0, sceneChanges.sceneChanges().size() - 1)
                .mapToObj(idx -> new Range(
                    offset + sceneChanges.sceneChanges().get(idx) * factor,
                    offset + sceneChanges.sceneChanges().get(idx + 1) * factor - 1
                )),
            Stream.of(new Range(offset + sceneChanges.sceneChanges().getLast() * factor, offset + sceneChanges.frameCount() * factor - 1))
        )
        .toList();
}

List<Range> makeGops(List<Range> scenes, int targetGopInFrames) {
    // No regular key-frames. On scene changes only
    if (targetGopInFrames <= 0) {
        return scenes;
    }
    return scenes
        .stream()
        .<Range>mapMulti((scene, sink) -> {
            // Scene is shorter than or equal to our target GOP. Keep it as GOP unchanged
            if (scene.count() <= targetGopInFrames) {
                sink.accept(scene);
                return;
            }
            // Scene is longer than our target GOP. Let's divide it in multiple GOPs of target length
            int gopCount = scene.count() / targetGopInFrames;
            int lastGop = scene.count() % targetGopInFrames;
            // Last GOP is of different length
            if (lastGop != 0) {
                // Last GOP is too short, let's concat it with previous in order to not to waste bits on I-frame
                if (3 * lastGop < targetGopInFrames) {
                    lastGop += targetGopInFrames;
                } else {
                    gopCount += 1;
                }
            } else {
                lastGop = targetGopInFrames;
            }

            for (int gop = 0; gop < gopCount; gop += 1) {
                sink.accept(Range.ofFromAndCount(
                    scene.from() + gop * targetGopInFrames,
                    gop == gopCount - 1 ? lastGop : targetGopInFrames
                ));
            }
        })
        .toList();
}

List<EncodingResult> encode(List<Range> ranges, String input, ProbeResult probeResult, Rational frameRate, EncodingParams encodingParams, int parallelism, int targetVmaf, int targetMinVmaf, String ffmpegPath) throws InterruptedException, ExecutionException {
    List<EncodingResult> encodedScenes = new ArrayList<>(ranges.size());
    try (ExecutorService executorService = Executors.newFixedThreadPool(parallelism)) {
        List<Future<EncodingResult>> encodings = new ArrayList<>(ranges.size());
        for (Range range : ranges) {
            encodings.add(executorService.submit(() -> {
                Path dir = Files.createDirectory(gopDir(encodingParams.encoder(), range, targetVmaf));
                EncodingResult result = encodeMatchingTargetVmafUsingBinarySearch(range, input, probeResult, frameRate, encodingParams, targetVmaf, targetMinVmaf, dir, ffmpegPath);
                try (Stream<Path> pathStream = Files.list(dir)) {
                    pathStream
                        .filter(Files::isRegularFile)
                        .filter(Predicate.not(result.file()::equals))
                        .forEach(path -> {
                            try {
                                Files.delete(path);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        });
                }
                return result;
            }));
        }

        for (var encoding : encodings) {
            encodedScenes.add(encoding.get());
        }
    }

    return encodedScenes;
}

Path gopDir(EncoderName encoder, Range range, int targetVmaf) {
    return Path.of("%s-range-%d-%d-vmaf%d".formatted(encoder, range.from(), range.to(), targetVmaf));
}

EncodingResult encodeMatchingTargetVmafUsingBinarySearch(Range range, String input, ProbeResult probeResult, Rational frameRate, EncodingParams encodingParams, int targetMeanVmaf, int targetMinVmaf, Path dir, String ffmpegPath) throws IOException, InterruptedException {
    EncoderName encoder = encodingParams.encoder();
    int l = encoder.effectiveCrfRange().from();
    int r = encoder.effectiveCrfRange().to();

    EncodingResult result = null;

    while (l <= r) {
        int crf = (l + r) / 2;

        EncodingIterationResult iterationResult = encode(range, input, probeResult, frameRate, encodingParams, crf, dir, ffmpegPath);

        double meanVmaf = iterationResult.vmaf().mean();
        double minVmaf = iterationResult.vmaf().min();

        result = new EncodingResult(range, iterationResult.file(), iterationResult.vmaf(), crf);

        if (meanVmaf >= targetMeanVmaf + 1) {
            if (targetMinVmaf > 0 && minVmaf < targetMinVmaf) {
                r = crf - 1;
            } else {
                l = crf + 1;
            }
        } else if (meanVmaf < targetMeanVmaf) {
            r = crf - 1;
        } else {
            IO.println("%s crf = %d Result vmaf = %s".formatted(range, crf, iterationResult.vmaf()));
            return result;
        }
    }

    if (result == null) {
        throw new IllegalStateException("Unexpectedly no result");
    }

    IO.println("%s [no match] crf = %d Result vmaf = %s".formatted(range, result.crf(), result.vmaf()));

    return result;
}

EncodingIterationResult encode(Range range, String input, ProbeResult probeResult, Rational frameRate, EncodingParams encodingParams, int crf, Path dir, String ffmpegPath) throws IOException, InterruptedException {
    String resultFilename = "%d-%d-crf%d-%s".formatted(range.from(), range.to(), crf, encodingParams.encoder());
    Path encodingFilename = dir.resolve("result-%s.mp4".formatted(resultFilename));
    Path vmafFilename = dir.resolve("vmaf-%s.json".formatted(resultFilename));

    double seek = probeResult.startTime + ((double) range.from()) / frameRate.numerator() * frameRate.denominator();
    int frameCount = range.count();

    GopEncodingParams gopEncodingParams = new GopEncodingParams(
        seek,
        input,
        frameCount,
        encodingParams.deinterlace(),
        encodingParams.encoder(),
        encodingParams.pixelFormat(),
        encodingParams.preset(),
        crf,
        encodingFilename.toString()
    );

    String formattedSeek = "%.2f".formatted(seek);
    for (int i = 0; i < 3; ++i) {
        Process encode = ProcessBuilder.startPipeline(List.of(
            ffmpegDecode(formattedSeek, input, frameCount, gopEncodingParams.deinterlace(), encodingParams, ffmpegPath),
            new ProcessBuilder()
                .command(encodeCommandForPipeInput(gopEncodingParams, ffmpegPath))
                .redirectError(ProcessBuilder.Redirect.DISCARD)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
        )).getLast();

        int encodeExit = encode.waitFor();

        if (encodeExit == 0) {
            break;
        }

        if (i < 2) {
            IO.println("Encode exited with %d. Retrying (attempt=%d)".formatted(encodeExit, i));
            continue;
        }

        throw new IllegalStateException("Encode exited with " + encodeExit);
    }
    IO.println("Encode finished");

    Vmaf vmaf = calculateVmaf(formattedSeek, frameRate, input, frameCount, encodingParams.deinterlace(), encodingParams, encodingFilename, vmafFilename, ffmpegPath);
    IO.println("%s: vmaf=%s".formatted(vmafFilename, vmaf));

    return new EncodingIterationResult(encodingFilename, vmaf.pooledMetrics().get("vmaf"));
}

Vmaf calculateVmaf(String seek, Rational frameRate, String input, int frameCount, DeinterlaceParams deinterlace, EncodingParams encodingParams, Path encoding, Path vmafFilename, String ffmpegPath) throws IOException, InterruptedException {
    int timeout = 30;

    for (int i = 0; i < 5; ++i) {
        List<String> cmd = command(
            ffBinary("ffmpeg", ffmpegPath),
            "-y",
            "-r", frameRate.toString(), "-i", encoding.toString(),
            "-r", frameRate.toString(), "-i", "-",
            "-filter_complex", "libvmaf=log_fmt=json:log_path=%s:n_threads=4".formatted(vmafFilename),
            "-f", "null", "-"
        );
        List<Process> vmaf = ProcessBuilder.startPipeline(List.of(
            ffmpegDecode(seek, input, frameCount, deinterlace, encodingParams, ffmpegPath),
            new ProcessBuilder()
                .command(
                    cmd
                )
                .redirectError(ProcessBuilder.Redirect.DISCARD)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
        ));

        if (!vmaf.getLast().waitFor(timeout, TimeUnit.MINUTES)) {
            vmaf.forEach(Process::destroyForcibly);
            vmaf.forEach(p -> {
                try {
                    p.waitFor();
                } catch (InterruptedException ie) {
                    // If interrupted; continue with next Process
                    Thread.currentThread().interrupt();
                }
            });

            IO.println("%s: %d-th attempt timed out. retrying..".formatted(vmafFilename, i));
            continue;
        }

        int exit = vmaf.getLast().exitValue();
        if (exit != 0) {
            throw new IllegalStateException("[" + encoding + "] vmaf exited with: " + exit);
        }

        IO.println("vmaf finished");

        return new ObjectMapper().readValue(vmafFilename.toFile(), Vmaf.class);
    }

    throw new InterruptedException("Attempts left");
}

private ProcessBuilder ffmpegDecode(String seek, String input, int frameCount, DeinterlaceParams deinterlace, EncodingParams encoding, String ffmpegPath) {
    return new ProcessBuilder()
        .command(command(
            ffBinary("ffmpeg", ffmpegPath), "-y", "-nostdin", "-ss", seek, "-i", input, "-frames:v", frameCount,
            filters(deinterlace, encoding),
            "-an",
            "-f", "yuv4mpegpipe", "-"
        ))
        .redirectError(ProcessBuilder.Redirect.DISCARD)
        ;
}

private List<String> filters(DeinterlaceParams deinterlace, EncodingParams encoding) {
    if (deinterlace == null || !deinterlace.isEnabled()) {
        return List.of();
    }

    DeinterlaceAlgorithm algorithm = deinterlace.algorithm();
    DeinterlaceMode mode = deinterlace.mode();
    String filter = switch (algorithm) {
        case BWDIF -> "bwdif=mode=%s".formatted(algorithm.mode(mode));
        case NNEDI -> "nnedi=weights=%s:field=%s".formatted(deinterlace.modelPath(), algorithm.mode(mode));
    };

    if (encoding.denoise()) {
        filter += ",hqdn3d";
    }

    return List.of("-vf", filter);
}

record GopEncodingParams(
    double seek,
    String input,
    int frameCount,
    DeinterlaceParams deinterlace,
    EncoderName encoder,
    PixelFormat pixelFormat,
    String preset,
    int crf,
    String output
) {
}

List<String> encodeCommand(GopEncodingParams params) {
    String seek = "%.2f".formatted(params.seek());

    EncoderName encoder = params.encoder();

    return command(
        "ffmpeg", "-y",
        "-nostdin",
        "-hide_banner",
        "-ss", seek,
        "-i", params.input(),
        "-frames:v", params.frameCount(),
        "-threads", "8",
//        deinterlace(params.deinterlace()),
        "-an",
        "-c:v", encoder, encoder.presetOption(), params.preset(),
        encoder.crfOption(), params.crf(), "-g", params.frameCount(),
        encoderSpecificParams(encoder, params.frameCount()),
        extraFfmpegParams(encoder, params.pixelFormat()),
        "-f", "mp4", params.output()
    );
}

List<String> encodeCommandForPipeInput(GopEncodingParams params, String ffmpegPath) {
    EncoderName encoder = params.encoder();

    return command(
        ffBinary("ffmpeg", ffmpegPath), "-y",
        "-hide_banner",
        "-i", "-",
        "-threads", "8",
        "-an",
        "-c:v", encoder, encoder.presetOption(), params.preset(),
        encoder.crfOption(), params.crf(), "-g", params.frameCount(),
        encoderSpecificParams(encoder, params.frameCount()),
        extraFfmpegParams(encoder, params.pixelFormat()),
        "-f", "mp4", params.output()
    );
}

List<String> encoderSpecificParams(EncoderName encoder, int gopInFrames) {
    if (encoder.encoderSpecificParamsOption() == null) {
        return List.of();
    }

    Map<String, String> gopParam = encoder.gopEncoderSpecificParams(gopInFrames);

    if (encoder.encoderSpecificParams().isEmpty() && gopParam.isEmpty()) {
        return List.of();
    }

    Map<String, String> params = new HashMap<>(encoder.encoderSpecificParams());
    params.putAll(gopParam);

    String encoderSpecificParams = params
        .entrySet()
        .stream()
        .map(e -> "%s=%s".formatted(e.getKey(), e.getValue()))
        .collect(Collectors.joining(":"));

    return List.of(encoder.encoderSpecificParamsOption(), encoderSpecificParams);
}

List<String> extraFfmpegParams(EncoderName encoder, PixelFormat pixelFormat) {
    Map<String, String> params = new LinkedHashMap<>(encoder.extraFfmpegParams());
    if (pixelFormat != PixelFormat.AUTO) {
        params.put("-pix_fmt", pixelFormat.toString());
    }
    return params
        .entrySet()
        .stream()
        .flatMap(e -> Stream.of(e.getKey(), e.getValue()))
        .toList();
}

List<String> command(Object... params) {
    return Stream.of(params)
        .<String>mapMulti((o, sink) -> {
            switch (o) {
                case null -> {}
                case CharSequence cs -> sink.accept(cs.toString());
                case Enum<?> e -> sink.accept(e.toString());
                case Integer i -> sink.accept(i.toString());
                case Long l -> sink.accept(l.toString());
                case Object[] a -> command(a).forEach(sink);
                case Collection<?> c -> command(c.toArray()).forEach(sink);
                default -> throw new IllegalStateException();
            }
        })
        .toList();
}

String concat(List<EncodingResult> encodedGops, EncoderName encoder, int targetVmaf, String ffmpegPath) throws IOException, InterruptedException {
    String concatInput = encodedGops.stream()
        .map(EncodingResult::file)
        .map("file '%s'"::formatted)
        .collect(Collectors.joining(System.lineSeparator()));

    IO.println("Concat.txt: %s".formatted(concatInput));

    String concatTxtFilename = "concat-%s-%s.txt".formatted(UUID.randomUUID(), encoder);
    Path concatTxtPath = Path.of(concatTxtFilename);
    Files.writeString(concatTxtPath, concatInput, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

    String concatenated = "concatenated-%s-vmaf%d.mp4".formatted(encoder, targetVmaf);
    Process concat = new ProcessBuilder()
        .command(ffBinary("ffmpeg", ffmpegPath), "-y", "-nostdin", "-hide_banner",
            "-f", "concat",
            "-safe", "0",
            "-i", concatTxtFilename,
            "-c", "copy",
            concatenated
        )
        .redirectError(ProcessBuilder.Redirect.DISCARD)
        .redirectOutput(ProcessBuilder.Redirect.DISCARD)
        .start();

    IO.println("Concat exit: %d".formatted(concat.waitFor()));

    Files.delete(concatTxtPath);

    return concatenated;
}

void mergeVideoAndAudio(String concatenated, Input source, String output, String ffmpegPath) throws IOException, InterruptedException {
    Process merge = new ProcessBuilder()
        .command(command(ffBinary("ffmpeg", ffmpegPath), "-y", "-nostdin", "-hide_banner",
            "-i", concatenated,
            seek(source.from()),
            duration(source.duration()),
            "-i", source.file(),
            "-c:v", "copy",
            "-c:a", "aac_at", "-b:a", "192K", "-ac", "2", "-ar", "48000",
            "-map", "0:v:0",
            "-map", "1:a:0",
            "-movflags", "+faststart",
            "-f", "mp4", output
        ))
        .redirectOutput(ProcessBuilder.Redirect.DISCARD)
        .redirectError(ProcessBuilder.Redirect.DISCARD)
        .start();

    int exit = merge.waitFor();
    IO.println("Merge exit: %d".formatted(exit));

    Path concatenatedPath = Path.of(concatenated);
    if (exit == 0) {
        Files.delete(concatenatedPath);
    } else {
        Files.move(concatenatedPath, Path.of(output), StandardCopyOption.ATOMIC_MOVE);
    }
}

Rational rationalFromString(String rational) {
    String[] components = rational.split("/");
    return new Rational(Long.parseLong(components[0]), Long.parseLong(components[1]));
}

void cleanup(List<Range> gops, EncodingParams encodingParams, int targetVmaf) throws IOException {
    for (Range gop : gops) {
        Path gopDir = gopDir(encodingParams.encoder(), gop, targetVmaf);
        try (Stream<Path> contents = Files.list(gopDir)) {
            for (Path path : (Iterable<Path>) (contents::iterator)) {
                Files.delete(path);
            }
        }
        Files.delete(gopDir);
    }
}

void makeReport(List<EncodingResult> encodedGops, Rational frameRate, String output, Duration processingDuration) throws IOException {
    record Report(
        Duration processingDuration,
        double minMeanVmaf,
        double meanMeanVmaf,
        double maxMeanVmaf,
        double minMinVmaf,
        int minBitrate,
        int meanBitrate,
        int maxBitrate,
        int minCrf,
        int maxCrf,
        int minGop,
        int maxGop,
        List<Gop> gops
    ) {
        record Gop(Range frames, Vmaf vmaf, int crf, int bitrateKbs) {
            record Vmaf(double min, double mean, double max) {
            }
        }
    }

    double minMeanVmaf = encodedGops.getFirst().vmaf().mean();
    long meanVmafWeightedSum = 0;
    long meanVmafWeightsSum = 0;
    double maxMeanVmaf = encodedGops.getFirst().vmaf().mean();
    double minMinVmaf = encodedGops.getFirst().vmaf().min();
    int minBitrate = bitrateKbs(encodedGops.getFirst().file(), encodedGops.getFirst().frames(), frameRate);
    int bitrateWeightedSum = bitrateKbs(encodedGops.getFirst().file(), encodedGops.getFirst().frames(), frameRate);
    int maxBitrate = bitrateKbs(encodedGops.getFirst().file(), encodedGops.getFirst().frames(), frameRate);
    int minCrf = encodedGops.getFirst().crf();
    int maxCrf = encodedGops.getFirst().crf();
    int minGop = encodedGops.getFirst().frames().count();
    int maxGop = encodedGops.getFirst().frames().count();

    List<Report.Gop> gops = new ArrayList<>(encodedGops.size());

    for (EncodingResult gopResult : encodedGops) {
        Range frames = gopResult.frames();
        int bitrateKbs = bitrateKbs(gopResult.file(), frames, frameRate);
        Vmaf.Agg vmaf = gopResult.vmaf();
        gops.add(new Report.Gop(
            frames,
            new Report.Gop.Vmaf(
                vmaf.min(),
                vmaf.mean(),
                vmaf.max()
            ),
            gopResult.crf(),
            bitrateKbs
        ));
        if (vmaf.mean() < minMeanVmaf) {
            minMeanVmaf = vmaf.mean();
        }
        meanVmafWeightedSum += (long) (vmaf.mean() * frames.count());
        meanVmafWeightsSum += frames.count();
        if (vmaf.mean() > maxMeanVmaf) {
            maxMeanVmaf = vmaf.mean();
        }
        if (vmaf.min() < minMinVmaf) {
            minMinVmaf = vmaf.min();
        }
        if (bitrateKbs < minBitrate) {
            minBitrate = bitrateKbs;
        }
        bitrateWeightedSum += bitrateKbs * frames.count();
        if (bitrateKbs > maxBitrate) {
            maxBitrate = bitrateKbs;
        }
        if (gopResult.crf() < minCrf) {
            minCrf = gopResult.crf();
        }
        if (gopResult.crf() > maxCrf) {
            maxCrf = gopResult.crf();
        }
        if (frames.count() < minGop) {
            minGop = frames.count();
        }
        if (frames.count() > maxGop) {
            maxGop = frames.count();
        }
    }

    Report report = new Report(
        processingDuration,
        minMeanVmaf,
        (double) meanVmafWeightedSum / meanVmafWeightsSum,
        maxMeanVmaf,
        minMinVmaf,
        minBitrate,
        (int) (bitrateWeightedSum / meanVmafWeightsSum),
        maxBitrate,
        minCrf,
        maxCrf,
        minGop,
        maxGop,
        gops
    );

    Path path = Path.of(Strings.CS.removeEnd(output, ".mp4") + ".json");

    new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .writeValue(path.toFile(), report);
}

int bitrateKbs(Path file, Range frames, Rational frameRate) throws IOException {
    long bytes = Files.size(file);
    long bits = bytes * 8;
    Rational seconds = Rational.divide(frames.count(), frameRate);
    return Math.toIntExact(bits / (seconds.numerator() * 1000 / seconds.denominator()));
}

Map<EncoderName, String> defaultPresets = Map.of(
    EncoderName.X264, "medium",
    EncoderName.X265, "medium",
    EncoderName.SVT_AV1, "6",
    EncoderName.VPX_VP9, "3",
    EncoderName.VV_ENC, "medium"
);

record ApplicationInfo(String title, String version, String vendor) {
    @Override
    public String toString() {
        return "%s version %s %s".formatted(title, version, vendor);
    }
}

// HH:MM:SS.MS
record DurationConverter() implements IStringConverter<Duration> {
    static Pattern pattern = Pattern.compile("^((?<HH>\\d+):)?((?<MM>[0-5]?\\d):)?(?<SS>[0-5]?\\d)(\\.(?<MS>\\d+))?$");

    @Override
    public Duration convert(String value) {
        Matcher matcher = pattern.matcher(value);

        if (!matcher.matches()) {
            throw new ParameterException("Invalid format. Expected: HH:MM:SS.MS");
        }

        return Duration.ofSeconds(Integer.parseInt(matcher.group("SS")))
            .plusMillis(Integer.parseInt(Optional.ofNullable(matcher.group("MS")).orElse("0")))
            .plusMinutes(Integer.parseInt(Optional.ofNullable(matcher.group("MM")).orElse("0")))
            .plusHours(Integer.parseInt(Optional.ofNullable(matcher.group("HH")).orElse("0")));
    }
}

class CommandLineArgs {
    @Parameter(
        names = {"-h", "-help"},
        help = true
    )
    boolean help;

    @Parameter(
        names = {"-i", "-input"},
        description = "Path to input video file",
        required = true,
        order = 0
    )
    String input;

    @Parameter(
        names = {"-o", "-output"},
        description = "Path to output video file",
        required = true,
        order = 1
    )
    String output;

    @Parameter(
        names = "-av-scenechange",
        description = "Path to av-scenechange utility",
        defaultValueDescription = "av-scenechange expected to present in PATH",
        order = 2
    )
    String avSceneChange = "av-scenechange";

    @Parameter(
        names = {"-target-vmaf", "-target-mean-vmaf"},
        description = "Target mean vmaf value, encoder have to achieve",
        order = 3
    )
    int targetVmaf = 90;

    @Parameter(
        names = "-target-min-vmaf",
        description = """
            Target min vmaf value, encoder have to achieve too as well as mean vmaf value. \
            Target min vmaf value achievement may result in higher mean vmaf value and higher bitrate\
            """,
        defaultValueDescription = "By default, we try to achieve only mean vmaf target value",
        order = 4
    )
    int targetMinVmaf = -1;

    @Parameter(
        names = "-target-gop-seconds",
        description = """
            Target GOP size in seconds a.k.a. regular key-frame placement interval. \
            For shorter scenes GOPs can be less than the target value. \
            '0' means key-frames will be placed on scene changes only (even if scenes so large)""",
        order = 5
    )
    int targetGopInSeconds = 15;

    @Parameter(
        names = "-encoder",
        description = "Encoder to use",
        order = 6
    )
    EncoderName encoder = EncoderName.SVT_AV1;

    @Parameter(
        names = "-preset",
        description = "Encoder preset",
        defaultValueDescription = """
            Encoder specific: 'medium' for libx264 & libx265, \
            '6' for libsvtav1 & '3' (cpu-used) for libvpx-vp9""",
        order = 7
    )
    String preset = "def";

    @Parameter(
        names = "-deinterlace",
        description = """
            Indicates that input video frames are interlaced (e.g. archive VHS) \
            and have to be deinterlaced with specified algorithm for correct encoding to progressive format""",
        order = 8
    )
    DeinterlaceAlgorithm deinterlaceAlgorithm;

    @Parameter(
        names = "-deinterlace-mode",
        description = "Deinterlace mode. 'send_frame' keeps source frame rate and 'send_field' doubles frame rate",
        order = 9
    )
    DeinterlaceMode deinterlaceMode = DeinterlaceMode.SEND_FRAME;

    @Parameter(
        names = "-deinterlace-model",
        description = """
            Path to model file if specified deinterlace algorithm requires one. \
            Currently makes sense only for 'nnedi'.
            The model file can be downloaded from here: \
            https://github.com/dubhater/vapoursynth-nnedi3/blob/master/src/nnedi3_weights.bin""",
        defaultValueDescription = "Model file with name 'nnedi3_weights.bin' expected to present at working dir",
        order = 10
    )
    String deinterlaceModelPath = "nnedi3_weights.bin";

    @Parameter(
        names = "-parallelism",
        description = """
            Amount of individual GOPs that will be processed in parallel. Allows to adjust CPU utilization. \
            Optimal value depends on expected CPU utilization as well as on environment, encoder, encoder preset, \
            source video resolution, complexity, etc""",
        order = 11
    )
    int parallelism = 6;

    @Parameter(
        names = {"-ss", "-from-time"},
        description = """
            Allows to encode only part of the video \
            starting from specified time in standard FFmpeg format: HH:MM:SS.MS""",
        converter = DurationConverter.class,
        order = 12
    )
    Duration fromTime;

    @Parameter(
        names = {"-t", "-duration"},
        description = """
            Allows to encode only part of the video \
            ending when specified duration reached in standard FFmpeg format: HH:MM:SS.MS""",
        converter = DurationConverter.class,
        order = 13
    )
    Duration duration;

    @Parameter(
        names = "-denoise",
        order = 15
    )
    boolean denoise;

    @Parameter(
        names = "-pix-fmt",
        order = 16
    )
    PixelFormat pixelFormat = PixelFormat.AUTO;

    @Parameter(
        names = "-ffmpeg-path",
        order = 17
    )
    String ffmpegPath;
}

enum EncoderName {
    X264(
        "libx264",
        "-crf", new Range(17, 51),
        "-preset",
        Map.of(),
        "-x264-params",
        Map.of()
    ),

    X265(
        "libx265",
        "-crf", new Range(17, 51),
        "-preset",
        Map.of("-tag:v", "hvc1"),
        "-x265-params",
        Map.of()
    ),

    VPX_VP9(
        "libvpx-vp9",
        "-crf", new Range(15, 63),
        "-cpu-used",
        Map.of(
            "-deadline", "good",
            "-lag-in-frames", "25",
            "-row-mt", "1"
        ),
        null,
        Map.of()
    ),

    SVT_AV1(
        "libsvtav1",
        "-crf", new Range(16, 63),
        "-preset",
        Map.of(),
        "-svtav1-params",
        Map.of(
            "lookahead", "120",
            "tune", "0"
        )
    ),

    VV_ENC(
        "libvvenc",
        "-qp", new Range(16, 63),
        "-preset",
        Map.of(),
        "-vvenc-params",
        Map.of("decodingrefreshtype", "idr")
    ) {
        @Override
        public Map<String, String> gopEncoderSpecificParams(int gopInFrames) {
            return Map.of(
                "intraperiod", Integer.toString(gopInFrames),
                "minintradistance", Integer.toString(gopInFrames)
            );
        }
    }

    ;

    private final String lib;
    private final String crfOption;
    private final Range effectiveCrfRange;
    private final String presetOption;
    private final Map<String, String> extraFfmpegParams;
    private final String encoderSpecificParamsOption;
    private final Map<String, String> encoderSpecificParams;

    EncoderName(
        String lib,
        String crfOption,
        Range effectiveCrfRange,
        String presetOption,
        Map<String, String> extraFfmpegParams,
        String encoderSpecificParamsOption,
        Map<String, String> encoderSpecificParams
    ) {
        this.lib = lib;
        this.crfOption = crfOption;
        this.effectiveCrfRange = effectiveCrfRange;
        this.presetOption = presetOption;
        this.extraFfmpegParams = extraFfmpegParams;
        this.encoderSpecificParamsOption = encoderSpecificParamsOption;
        this.encoderSpecificParams = encoderSpecificParams;
    }

    @Override
    public String toString() {
        return lib;
    }

    String crfOption() {
        return crfOption;
    }

    Range effectiveCrfRange() {
        return effectiveCrfRange;
    }

    public String presetOption() {
        return presetOption;
    }

    public Map<String, String> extraFfmpegParams() {
        return extraFfmpegParams;
    }

    public String encoderSpecificParamsOption() {
        return encoderSpecificParamsOption;
    }

    public Map<String, String> encoderSpecificParams() {
        return encoderSpecificParams;
    }

    public Map<String, String> gopEncoderSpecificParams(int gopInFrames) {
        return Map.of();
    }
}

enum DeinterlaceAlgorithm {
    BWDIF(DeinterlaceMode.SEND_FRAME.toString(), DeinterlaceMode.SEND_FIELD.toString()),
    NNEDI("a", "af"),
    ;
    private final String sendFrameMode;
    private final String sendFieldMode;

    DeinterlaceAlgorithm(String sendFrameMode, String sendFieldMode) {
        this.sendFrameMode = sendFrameMode;
        this.sendFieldMode = sendFieldMode;
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }

    public String mode(DeinterlaceMode mode) {
        return switch (mode) {
            case SEND_FRAME -> sendFrameMode;
            case SEND_FIELD -> sendFieldMode;
        };
    }
}

enum DeinterlaceMode {
    SEND_FRAME,
    SEND_FIELD,
    ;

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}

record Input(
    String file,
    Duration from,
    Duration duration
) {
}

record ProbeResult(int frameCount, Rational frameRate, double startTime, double duration) {
}

@JsonIgnoreProperties(ignoreUnknown = true)
record SceneChanges(
    @JsonProperty("scene_changes") List<Integer> sceneChanges,
    @JsonProperty("frame_count") int frameCount
) {
}

record EncodingParams(
    EncoderName encoder,
    String preset,
    DeinterlaceParams deinterlace,
    boolean denoise,
    PixelFormat pixelFormat
) {
}

record DeinterlaceParams(
    DeinterlaceAlgorithm algorithm,
    DeinterlaceMode mode,
    String modelPath
) {
    boolean isEnabled() {
        return algorithm() != null;
    }

    boolean doublesFrameRate() {
        return isEnabled() && mode == DeinterlaceMode.SEND_FIELD;
    }
}

record Range(int from, int to) {
    public Range {
        if (from > to) {
            throw new IllegalArgumentException("to MUST be larger than or equal to from. Actually was: from=%d, to=%d".formatted(from, to));
        }
    }

    public static Range ofFromAndCount(int from, int count) {
        return new Range(from, from + count - 1);
    }

    public int count() {
        return to - from + 1;
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
record Vmaf(@JsonProperty("pooled_metrics") Map<String, Agg> pooledMetrics) {
    public record Agg(
        double min,
        double max,
        double mean,
        @JsonProperty("harmonic_mean") double harmonicMean
    ) {
    }
}

record EncodingIterationResult(Path file, Vmaf.Agg vmaf) {
}

record EncodingResult(Range frames, Path file, Vmaf.Agg vmaf, int crf) {
}

record Rational(long numerator, long denominator) {
    @Override
    public String toString() {
        return numerator + "/" + denominator;
    }

    public long multiply(long factor) {
        return factor * numerator / denominator;
    }

    public static Rational divide(long value, Rational rational) {
        return new Rational(value * rational.denominator(), rational.numerator());
    }
}

enum PixelFormat {
    AUTO,
    YUV420P,
    YUV420P10LE,
    ;

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}
