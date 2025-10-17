import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    String avSceneChange = commandLineArgs.avSceneChange;
    int targetGopInSeconds = commandLineArgs.targetGopInSeconds;
    EncoderName encoder = commandLineArgs.encoder;
    int targetVmaf = commandLineArgs.targetVmaf;
    String output = commandLineArgs.output;
    int parallelism = commandLineArgs.parallelism;

    ProbeResult probeResult = ffprobe(input);
    IO.println("Probe result: %s".formatted(probeResult));
    SceneChanges sceneChanges = detectSceneChanges(avSceneChange, input);
    IO.println("Scene changes: %s".formatted(sceneChanges));

    validate(probeResult, sceneChanges);

    int targetGopInFrames = targetGopInSeconds * probeResult.frameRate().numerator() / probeResult.frameRate().denominator();
    List<Range> scenes = makeScenes(sceneChanges);
    IO.println("Scenes: %s".formatted(scenes));

    List<Range> gops = makeGops(scenes, targetGopInFrames);
    IO.println("GOPs: %s".formatted(gops));

    List<EncodingResult> encodedGops = encode(gops, input, probeResult.frameRate(), encoder, parallelism, targetVmaf);
    IO.println("Encoded gops: %s".formatted(encodedGops));

    String concatenated = concat(encodedGops, encoder, targetVmaf);

    mergeVideoAndAudio(concatenated, input, output);
}

private CommandLineArgs parseCommandLineArgs(String[] args) {
    CommandLineArgs commandLineArgs = new CommandLineArgs();
    JCommander.newBuilder()
        .addObject(commandLineArgs)
        .build()
        .parse(args);
    return commandLineArgs;
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

List<Range> makeScenes(SceneChanges sceneChanges) {
    return Stream.concat(
            IntStream.range(0, sceneChanges.sceneChanges().size() - 1)
                .mapToObj(idx -> new Range(
                    sceneChanges.sceneChanges().get(idx),
                    sceneChanges.sceneChanges().get(idx + 1) - 1
                )),
            Stream.of(new Range(sceneChanges.sceneChanges().getLast(), sceneChanges.frameCount() - 1))
        )
        .toList();
}

List<Range> makeGops(List<Range> scenes, int targetGopInFrames) {
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

List<EncodingResult> encode(List<Range> ranges, String input, Rational frameRate, EncoderName enc, int parallelism, int targetVmaf) throws InterruptedException, ExecutionException {
    List<EncodingResult> encodedScenes = new ArrayList<>(ranges.size());
    try (ExecutorService executorService = Executors.newFixedThreadPool(parallelism)) {
        List<Future<EncodingResult>> encodings = new ArrayList<>(ranges.size());
        for (Range range : ranges) {
            encodings.add(executorService.submit(() -> {
                Path dir = Files.createDirectory(Path.of("range-%d-%s-%s-vmaf%d".formatted(range.from(), range.to(), enc, targetVmaf)));
                EncodingResult result = encodeMatchingTargetVmafUsingBinarySearch(range, input, frameRate, enc, targetVmaf, dir);
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

EncodingResult encodeMatchingTargetVmafUsingBinarySearch(Range range, String input, Rational frameRate, EncoderName encoderName, int targetVmaf, Path dir) {
    int l = encoderName.effectiveCrfRange().from();
    int r = encoderName.effectiveCrfRange().to();

    EncodingResult lastRes = null;

    while (l <= r) {
        int crf = (l + r) / 2;

        EncodingIterationResult result;
        try {
            result = encode(range, input, frameRate, encoderName, crf, dir);
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

EncodingIterationResult encode(Range range, String input, Rational frameRate, EncoderName encoderName, int crf, Path dir) throws IOException, InterruptedException {
    String resultFilename = "%d-%d-crf%d-%s".formatted(range.from(), range.to(), crf, encoderName);

    Path encodingFilename = dir.resolve("result-%s.mp4".formatted(resultFilename));
    Path vmafFilename = dir.resolve("vmaf-%s.json".formatted(resultFilename));

    String seek = "%.2f".formatted(((double) range.from()) / frameRate.numerator() * frameRate.denominator());
    int frames = range.count();

    Process encode = new ProcessBuilder()
        .command(command(encoderName).command(seek, input, frames, crf, frames, encodingFilename.toString()))
        .inheritIO()
        .start();

    int encodeExit = encode.waitFor();
    IO.println("Encode exit code: %d".formatted(encodeExit));

    Vmaf vmaf = calculateVmaf(seek, frameRate, input, frames, encodingFilename, vmafFilename);
    IO.println("%s: vmaf=%s".formatted(vmafFilename, vmaf));

    return new EncodingIterationResult(encodingFilename, vmaf.pooledMetrics().get("vmaf"));
}

Vmaf calculateVmaf(String seek, Rational frameRate, String input, int frameCount, Path encoding, Path vmafFilename) throws IOException, InterruptedException {
    int timeout = 30;

    for (int i = 0; i < 5; ++i) {
        List<Process> vmaf = ProcessBuilder.startPipeline(List.of(
            new ProcessBuilder()
                .command("ffmpeg", "-y", "-nostdin", "-ss", seek, "-i", input,
                    "-frames:v", String.valueOf(frameCount),
                    "-an", "-f", "yuv4mpegpipe", "-"
                ),
            new ProcessBuilder()
                .command("ffmpeg",
                    "-y",
                    "-r", frameRate.toString(), "-i", encoding.toString(),
                    "-r", frameRate.toString(), "-i", "-",
                    "-filter_complex", "libvmaf=log_fmt=json:log_path=%s:n_threads=4".formatted(vmafFilename),
                    "-f", "null", "-"
                ).redirectOutput(ProcessBuilder.Redirect.DISCARD)
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

String[] svtav1(String seek, String input, int frameCount, int crf, int gopInFrames, String output) {
    return command("ffmpeg", "-y", "-nostdin", "-ss", seek, "-i", input,
        "-frames:v", String.valueOf(frameCount),
        "-threads", "8",
        "-an", "-c:v", "libsvtav1", "-crf", String.valueOf(crf), "-preset", "6", "-g", String.valueOf(gopInFrames), "-svtav1-params", "lookahead=120",
        "-f", "mp4", output);
}

String[] x264(String seek, String input, int frameCount, int crf, int gopInFrames, String output) {
    return command("ffmpeg", "-y", "-nostdin", "-ss", seek, "-i", input,
        "-frames:v", String.valueOf(frameCount),
        "-threads", "8",
        "-an", "-c:v", "libx264", "-crf",String.valueOf(crf), "-preset", "ultrafast", "-g", String.valueOf(gopInFrames),
        "-f", "mp4", output);
}

String[] x265(String seek, String input, int frameCount, int crf, int gopInFrames, String output) {
    return command("ffmpeg", "-y", "-nostdin", "-ss", seek, "-i", input,
        "-frames:v", String.valueOf(frameCount),
        "-threads", "8",
        "-an", "-c:v", "libx265", "-crf", String.valueOf(crf), "-preset", "slow", "-g", String.valueOf(gopInFrames),
        "-tag:v", "hvc1",
        "-f", "mp4", output);
}

String[] vpxvp9(String seek, String input, int frameCount, int crf, int gopInFrames, String output) {
    return command("ffmpeg", "-y", "-nostdin", "-ss", seek, "-i", input,
        "-frames:v", String.valueOf(frameCount),
        "-threads", "8",
        "-an",
        "-c:v", "libvpx-vp9", "-crf", String.valueOf(crf), "-cpu-used", "1", "-deadline", "good", "-lag-in-frames", "25", "-row-mt", "1", "-g", String.valueOf(gopInFrames),
        "-f", "mp4", output);
}

String[] command(String... args) {
    return args;
}

EncodingCommand command(EncoderName encoderName) {
    return switch (encoderName) {
        case X264 -> this::x264;
        case X265 -> this::x265;
        case VPX_VP9 -> this::vpxvp9;
        case SVT_AV1 -> this::svtav1;
    };
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
    return new Rational(Integer.parseInt(components[0]), Integer.parseInt(components[1]));
}

class CommandLineArgs {
    @Parameter(names = {"-i", "--input"}, description = "Path to input video file", required = true)
    String input;

    @Parameter(names = {"-o", "--output"}, description = "Path to output video file", required = true)
    String output;

    @Parameter(names = {"-tv", "--target-vmaf"}, description = "Target vmaf value", required = true)
    int targetVmaf;

    @Parameter(names = {"-tgs", "--target-gop-seconds"}, description = "Target GOP size in seconds", required = true)
    int targetGopInSeconds;

    @Parameter(names = {"-avsc", "--av-scenechange"}, description = "Path to av-scenechange utility", required = true)
    String avSceneChange;

    @Parameter(names = {"-e", "--enc"}, description = "Encoder to use", required = true)
    EncoderName encoder;

    @Parameter(names = {"-p", "--parallelism"}, description = "Parallel GOP encoding count", required = true)
    int parallelism;
}

enum EncoderName {
    X264("libx264", 17, 48),
    X265("libx265", 17, 48),
    VPX_VP9("libvpx-vp9", 15, 46),
    SVT_AV1("libsvtav1", 16, 47),
    ;

    private final String lib;
    private final Range effectiveCrfRange;

    EncoderName(String lib, int effectiveCrfRangeFrom, int effectiveCrfRangeTo) {
        this(lib, new Range(effectiveCrfRangeFrom, effectiveCrfRangeTo));
    }

    EncoderName(String lib, Range effectiveCrfRange) {
        this.lib = lib;
        this.effectiveCrfRange = effectiveCrfRange;
    }

    @Override
    public String toString() {
        return lib;
    }

    Range effectiveCrfRange() {
        return effectiveCrfRange;
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

record Range(int from, int to) {
    public Range {
        if (from >= to) {
            throw new IllegalArgumentException("to MUST be larger than from. Actually was: from=%d, to=%d".formatted(from, to));
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

@FunctionalInterface
interface EncodingCommand {
    String[] command(String seek, String input, int frameCount, int crf, int gopInFrames, String output);
}

record Rational(int numerator, int denominator) {
    @Override
    public String toString() {
        return numerator + "/" + denominator;
    }
}
