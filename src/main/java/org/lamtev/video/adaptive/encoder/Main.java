import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;


void main(String... args) throws IOException, InterruptedException, ExecutionException {
    CommandLineArgs commandLineArgs = parseCommandLineArgs(args);

    String input = commandLineArgs.input;
    String output = commandLineArgs.output;
    String avSceneChange = commandLineArgs.avSceneChange;
    int targetVmaf = commandLineArgs.targetVmaf;
    int targetGopInSeconds = commandLineArgs.targetGopInSeconds;
    EncoderName encoder = commandLineArgs.encoder;
    String preset = commandLineArgs.preset;
    DeinterlaceAlgorithm deinterlace = commandLineArgs.deinterlaceAlgorithm;
    DeinterlaceMode deinterlaceMode = commandLineArgs.deinterlaceMode;
    String deinterlaceModelPath = commandLineArgs.deinterlaceModelPath;
    DeinterlaceParams deinterlaceParams = new DeinterlaceParams(deinterlace, deinterlaceMode, deinterlaceModelPath);

    EncodingParams encodingParams = new EncodingParams(encoder, preset, deinterlaceParams);
    int parallelism = commandLineArgs.parallelism;

    ProbeResult probeResult = ffprobe(input);
    IO.println("Probe result: %s".formatted(probeResult));
    SceneChanges sceneChanges = detectSceneChanges(avSceneChange, input);
    IO.println("Scene changes: %s".formatted(sceneChanges));

    validate(probeResult, sceneChanges);

    Rational frameRate = probeResult.frameRate();
    if (deinterlaceParams.doublesFrameRate()) {
        frameRate = new Rational(frameRate.numerator() * 2, frameRate.denominator());
    }

    int targetGopInFrames = Math.toIntExact(frameRate.multiply(targetGopInSeconds));
    List<Range> scenes = makeScenes(sceneChanges, deinterlaceParams);
    IO.println("Scenes: %s".formatted(scenes));

    List<Range> gops = makeGops(scenes, targetGopInFrames);
    IO.println("GOPs: %s".formatted(gops));

    List<EncodingResult> encodedGops = encode(gops, input, frameRate, encodingParams, parallelism, targetVmaf);
    IO.println("Encoded gops: %s".formatted(encodedGops));

    String concatenated = concat(encodedGops, encoder, targetVmaf);

    mergeVideoAndAudio(concatenated, input, output);
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

ProbeResult ffprobe(String input) throws IOException {
    Process ffprobe = new ProcessBuilder()
        .command("ffprobe",
            "-select_streams", "v",
            "-show_streams", "-count_packets",
            "-print_format", "json", input)
        .start();

    @JsonIgnoreProperties(ignoreUnknown = true)
    record Out(List<Stream> streams) {
        @JsonIgnoreProperties(ignoreUnknown = true)
        record Stream(Integer nb_frames, Integer nb_read_packets, String avg_frame_rate, String r_frame_rate) {
        }
    }

    try (Reader stdout = ffprobe.inputReader()) {
        Out res = new ObjectMapper().readValue(stdout, Out.class);
        Out.Stream stream = res.streams().getFirst();
        return new ProbeResult(
            Optional.ofNullable(stream.nb_frames())
                .orElse(stream.nb_read_packets()),
            rationalFromString(
                Optional.ofNullable(stream.avg_frame_rate())
                    .orElse(stream.r_frame_rate())
            )
        );
    }
}

private static void validate(ProbeResult probeResult, SceneChanges sceneChanges) {
    if (probeResult.frameCount() != sceneChanges.frameCount()) {
        throw new IllegalStateException("ffprobe and av-scenechange result mismatch: ffprobe.frameCount=%d != av-scenechange.frameCount=%d".formatted(probeResult.frameCount(), sceneChanges.frameCount()));
    }
}

SceneChanges detectSceneChanges(String avSceneChange, String input) throws IOException, InterruptedException {
    String scenesJsonFile = "scenes-%s.json".formatted(UUID.randomUUID());

    // We read file using ffmpeg directly because av-scenechange's demuxing/decoding are buggy.
    // Sometimes av-scenechange demuxes/decodes less frames than input coded stream contains.
    List<Process> pipeline = ProcessBuilder.startPipeline(List.of(
        new ProcessBuilder()
            .command("ffmpeg", "-i", input, "-an", "-f", "yuv4mpegpipe", "-"),
        new ProcessBuilder()
            .command(avSceneChange, "-o", scenesJsonFile, "-")
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

    return sceneChanges;
}

List<Range> makeScenes(SceneChanges sceneChanges, DeinterlaceParams deinterlace) {
    int factor = deinterlace.doublesFrameRate() ? 2 : 1;
    return Stream.concat(
            IntStream.range(0, sceneChanges.sceneChanges().size() - 1)
                .mapToObj(idx -> new Range(
                    sceneChanges.sceneChanges().get(idx) * factor,
                    sceneChanges.sceneChanges().get(idx + 1) * factor - 1
                )),
            Stream.of(new Range(sceneChanges.sceneChanges().getLast() * factor, sceneChanges.frameCount() * factor - 1))
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
                gopCount += 1;
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

List<EncodingResult> encode(List<Range> ranges, String input, Rational frameRate, EncodingParams encodingParams, int parallelism, int targetVmaf) throws InterruptedException, ExecutionException {
    List<EncodingResult> encodedScenes = new ArrayList<>(ranges.size());
    try (ExecutorService executorService = Executors.newFixedThreadPool(parallelism)) {
        List<Future<EncodingResult>> encodings = new ArrayList<>(ranges.size());
        for (Range range : ranges) {
            encodings.add(executorService.submit(() -> {
                Path dir = Files.createDirectory(Path.of("range-%d-%s-%s-vmaf%d".formatted(range.from(), range.to(), encodingParams.encoder(), targetVmaf)));
                EncodingResult result = encodeMatchingTargetVmafUsingBinarySearch(range, input, frameRate, encodingParams, targetVmaf, dir);
                try (Stream<Path> pathStream = Files.walk(dir)) {
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

EncodingResult encodeMatchingTargetVmafUsingBinarySearch(Range range, String input, Rational frameRate, EncodingParams encodingParams, int targetVmaf, Path dir) {
    EncoderName encoder = encodingParams.encoder();
    int l = encoder.effectiveCrfRange().from();
    int r = encoder.effectiveCrfRange().to();

    EncodingResult lastRes = null;

    while (l <= r) {
        int crf = (l + r) / 2;

        EncodingIterationResult result;
        try {
            result = encode(range, input, frameRate, encodingParams, crf, dir);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }

        double vmaf = result.vmaf().mean();

        lastRes = new EncodingResult(result.file(), result.vmaf(), crf);

        if (vmaf >= targetVmaf + 1) {
            l = crf + 1;
        } else if (vmaf < targetVmaf) {
            r = crf - 1;
        } else {
            IO.println("%s crf = %d Result vmaf = %s".formatted(range, crf, result.vmaf()));
            return lastRes;
        }
    }

    if (lastRes == null) {
        throw new IllegalStateException("Unexpectedly no result");
    }

    IO.println("%s [no match] crf = %d Result vmaf = %s".formatted(range, lastRes.crf(), lastRes.vmaf()));

    return lastRes;
}

EncodingIterationResult encode(Range range, String input, Rational frameRate, EncodingParams encodingParams, int crf, Path dir) throws IOException, InterruptedException {
    String resultFilename = "%d-%d-crf%d-%s".formatted(range.from(), range.to(), crf, encodingParams.encoder());
    Path encodingFilename = dir.resolve("result-%s.mp4".formatted(resultFilename));
    Path vmafFilename = dir.resolve("vmaf-%s.json".formatted(resultFilename));

    double seek = ((double) range.from()) / frameRate.numerator() * frameRate.denominator();
    int frameCount = range.count();

    GopEncodingParams gopEncodingParams = new GopEncodingParams(
        seek,
        input,
        frameCount,
        encodingParams.deinterlace(),
        encodingParams.encoder(),
        encodingParams.preset(),
        crf,
        encodingFilename.toString()
    );

    Process encode = new ProcessBuilder()
        .command(encodeCommand(gopEncodingParams))
        .inheritIO()
        .start();

    int encodeExit = encode.waitFor();
    IO.println("Encode exit code: %d".formatted(encodeExit));

    Vmaf vmaf = calculateVmaf("%.2f".formatted(seek), frameRate, input, frameCount, encodingParams.deinterlace(), encodingFilename, vmafFilename);
    IO.println("%s: vmaf=%s".formatted(vmafFilename, vmaf));

    return new EncodingIterationResult(encodingFilename, vmaf.pooledMetrics().get("vmaf"));
}

Vmaf calculateVmaf(String seek, Rational frameRate, String input, int frameCount, DeinterlaceParams deinterlace, Path encoding, Path vmafFilename) throws IOException, InterruptedException {
    int timeout = 30;

    for (int i = 0; i < 5; ++i) {
        List<Process> vmaf = ProcessBuilder.startPipeline(List.of(
            new ProcessBuilder()
                .command(command(
                    "ffmpeg", "-y", "-nostdin", "-ss", seek, "-i", input, "-frames:v", frameCount,
                    deinterlace(deinterlace),
                    "-an",
                    "-f", "yuv4mpegpipe", "-"
                )),
            new ProcessBuilder()
                .command(
                    "ffmpeg",
                    "-y",
                    "-r", frameRate.toString(), "-i", encoding.toString(),
                    "-r", frameRate.toString(), "-i", "-",
                    "-filter_complex", "libvmaf=log_fmt=json:log_path=%s:n_threads=4".formatted(vmafFilename),
                    "-f", "null", "-"
                )
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

        IO.println("vmaf exit = %d".formatted(exit));

        return new ObjectMapper().readValue(vmafFilename.toFile(), Vmaf.class);
    }

    throw new InterruptedException("Attempts left");
}

record GopEncodingParams(
    double seek,
    String input,
    int frameCount,
    DeinterlaceParams deinterlace,
    EncoderName encoder,
    String preset,
    int crf,
    String output
) {
}

List<String> encodeCommand(GopEncodingParams params) {
    String seek = "%.2f".formatted(params.seek());

    EncoderName encoder = params.encoder();

    String encoderSpecificParams = encoder.encoderSpecificParams()
        .entrySet()
        .stream()
        .map(e -> "%s=%s".formatted(e.getKey(), e.getValue()))
        .collect(Collectors.joining(":"));

    List<String> extraFfmpegParams = encoder.extraFfmpegParams()
        .entrySet()
        .stream()
        .flatMap(e -> Stream.of(e.getKey(), e.getValue()))
        .toList();

    return command(
        "ffmpeg", "-y", "-nostdin",
        "-ss", seek, "-i", params.input(), "-frames:v", params.frameCount(),
        "-threads", "8",
        deinterlace(params.deinterlace()),
        "-an",
        "-c:v", encoder, encoder.presetOption(), params.preset(),
        "-crf", params.crf(), "-g", params.frameCount(),
        encoder.encoderSpecificParamsOption(), encoderSpecificParams,
        extraFfmpegParams,
        "-f", "mp4", params.output()
    );
}

List<String> deinterlace(DeinterlaceParams deinterlace) {
    if (!deinterlace.isEnabled()) {
        return List.of();
    }
    DeinterlaceAlgorithm algorithm = deinterlace.algorithm();
    DeinterlaceMode mode = deinterlace.mode();
    String filter = switch (algorithm) {
        case BWDIF -> "bwdif=mode=%s".formatted(algorithm.mode(mode));
        case NNEDI -> "nnedi=weights=%s:field=%s".formatted(deinterlace.modelPath(), algorithm.mode(mode));
    };
    return List.of("-vf", filter);
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

String concat(List<EncodingResult> encodedGops, EncoderName encoder, int targetVmaf) throws IOException, InterruptedException {
    String concatInput = encodedGops.stream()
        .map(EncodingResult::file)
        .map("file '%s'"::formatted)
        .collect(Collectors.joining(System.lineSeparator()));

    IO.println("Concat.txt: %s".formatted(concatInput));

    String concatTxtFilename = "concat-%s-%s.txt".formatted(UUID.randomUUID(), encoder);
    Files.writeString(Path.of(concatTxtFilename), concatInput, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

    String concatenated = "concatenated-%s-vmaf%d.mp4".formatted(encoder, targetVmaf);
    Process concat = new ProcessBuilder()
        .command("ffmpeg", "-f", "concat", "-safe", "0", "-i", concatTxtFilename, "-c", "copy", concatenated)
        .inheritIO()
        .start();

    IO.println("Concat exit: %d".formatted(concat.waitFor()));

    return concatenated;
}

void mergeVideoAndAudio(String concatenated, String source, String output) throws IOException, InterruptedException {
    Process merge = new ProcessBuilder()
        .command("ffmpeg", "-y", "-nostdin", "-i", concatenated, "-i", source,
            "-c:v", "copy",
            "-c:a", "aac_at", "-b:a", "192K", "-ac", "2", "-ar", "48000",
            "-map", "0:v:0",
            "-map", "1:a:0",
            "-movflags", "+faststart",
            "-f", "mp4", output)
        .start();

    IO.println("Merge exit: %d".formatted(merge.waitFor()));
}

Rational rationalFromString(String rational) {
    String[] components = rational.split("/");
    return new Rational(Long.parseLong(components[0]), Long.parseLong(components[1]));
}

Map<EncoderName, String> defaultPresets = Map.of(
    EncoderName.X264, "medium",
    EncoderName.X265, "medium",
    EncoderName.SVT_AV1, "6",
    EncoderName.VPX_VP9, "3"
);

record ApplicationInfo(String title, String version, String vendor) {
    @Override
    public String toString() {
        return "%s version %s %s".formatted(title, version, vendor);
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
        names = "-target-vmaf",
        description = "Target vmaf value, encoder have to achieve",
        order = 3
    )
    int targetVmaf = 90;

    @Parameter(
        names = "-target-gop-seconds",
        description = """
            Target GOP size in seconds a.k.a. regular key-frame placement interval. \
            For shorter scenes GOPs can be less than the target value. \
            '0' means key-frames will be placed on scene changes only (even if scenes so large)""",
        order = 4
    )
    int targetGopInSeconds = 15;

    @Parameter(
        names = "-encoder",
        description = "Encoder to use",
        order = 5
    )
    EncoderName encoder = EncoderName.SVT_AV1;

    @Parameter(
        names = "-preset",
        description = "Encoder preset",
        defaultValueDescription = """
            Encoder specific: 'medium' for libx264 & libx265, \
            '6' for libsvtav1 & '3' (cpu-used) for libvpx-vp9""",
        order = 6
    )
    String preset = "def";

    @Parameter(
        names = "-deinterlace",
        description = """
            Indicates that input video frames are interlaced (e.g. archive VHS) \
            and have to be deinterlaced with specified algorithm for correct encoding to progressive format""",
        order = 7
    )
    DeinterlaceAlgorithm deinterlaceAlgorithm;

    @Parameter(
        names = "-deinterlace-mode",
        description = "Deinterlace mode. 'send_frame' keeps source frame rate and 'send_field' doubles frame rate",
        order = 8
    )
    DeinterlaceMode deinterlaceMode = DeinterlaceMode.SEND_FIELD;

    @Parameter(
        names = "-deinterlace-model",
        description = """
            Path to model file if specified deinterlace algorithm requires one. \
            Currently makes sense only for 'nnedi'.
            The model file can be downloaded from here: \
            https://github.com/dubhater/vapoursynth-nnedi3/blob/master/src/nnedi3_weights.bin""",
        defaultValueDescription = "Model file with name 'nnedi3_weights.bin' expected to present at working dir",
        order = 9
    )
    String deinterlaceModelPath = "nnedi3_weights.bin";

    @Parameter(
        names = "-parallelism",
        description = """
            Amount of individual GOPs that will be processed in parallel. Allows to adjust CPU utilization. \
            Optimal value depends on expected CPU utilization as well as on environment, encoder, encoder preset, \
            source video resolution, complexity, etc""",
        order = 10
    )
    int parallelism = 6;
}

enum EncoderName {
    X264(
        "libx264",
        new Range(17, 51),
        "-preset",
        Map.of(),
        "-x264-params",
        Map.of()
    ),

    X265(
        "libx265",
        new Range(17, 51),
        "-preset",
        Map.of("-tag:v", "hvc1"),
        "-x265-params",
        Map.of()
    ),

    VPX_VP9(
        "libvpx-vp9",
        new Range(15, 63),
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
        new Range(16, 63),
        "-preset",
        Map.of(),
        "-svtav1-params",
        Map.of("lookahead", "120")
    ),
    ;

    private final String lib;
    private final Range effectiveCrfRange;
    private final String presetOption;
    private final Map<String, String> extraFfmpegParams;
    private final String encoderSpecificParamsOption;
    private final Map<String, String> encoderSpecificParams;

    EncoderName(
        String lib,
        Range effectiveCrfRange,
        String presetOption,
        Map<String, String> extraFfmpegParams,
        String encoderSpecificParamsOption,
        Map<String, String> encoderSpecificParams
    ) {
        this.lib = lib;
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

record ProbeResult(int frameCount, Rational frameRate) {
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
    DeinterlaceParams deinterlace
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

record EncodingResult(Path file, Vmaf.Agg vmaf, int crf) {
}

record Rational(long numerator, long denominator) {
    @Override
    public String toString() {
        return numerator + "/" + denominator;
    }

    public long multiply(long factor) {
        return factor * numerator / denominator;
    }
}
