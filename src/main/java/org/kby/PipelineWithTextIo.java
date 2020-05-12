package org.kby;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;
import org.slf4j.Logger;

import static org.apache.beam.sdk.io.FileSystems.matchNewResource;
import static org.apache.beam.sdk.transforms.SerializableFunctions.identity;
import static org.joda.time.Duration.ZERO;
import static org.joda.time.Duration.standardSeconds;


public class PipelineWithTextIo {

    private static final Logger log = org.slf4j.LoggerFactory.getLogger(PipelineWithTextIo.class);

    private static final String FILES_FOLDER = "pipe_with_lateness_%ss/files";
    private static final String FILES_AFTER_DISTINCT_FOLDER = "pipe_with_lateness_%ss/files-after-distinct";

    private static final ResourceId temp = FileSystems.matchNewResource("temp", true);

    private static final Duration WITHOUT_LATENESS = ZERO;
    private static final Duration ONE_MINUTE = standardSeconds(60L);

    public static void main(String[] args) {
        Duration lateness = args.length > 0 ? standardSeconds(Long.parseLong(args[0])) : ZERO;

        runPipeline(lateness);
    }

    public static void runPipeline(Duration lateness) {

        PipelineOptions options = PipelineOptionsFactory.fromArgs("--runner=FlinkRunner").create();

        String filesPath = String.format(FILES_FOLDER, lateness.getStandardSeconds());
        TextIO.TypedWrite<String, Void> writeData = TextIO.<String>writeCustomType().to(new FilenamePolicy(filesPath))
                .withFormatFunction(identity())
                .withTempDirectory(temp)
                .withNumShards(1)
                .withWindowedWrites();

        String filesAfterDistinct = String.format(FILES_AFTER_DISTINCT_FOLDER, lateness.getStandardSeconds());
        TextIO.TypedWrite<String, Void> writeFiles = TextIO.<String>writeCustomType().to(new FilenamePolicy(filesAfterDistinct))
                .withFormatFunction(identity())
                .withTempDirectory(temp)
                .withNumShards(1)
                .withWindowedWrites();

        Pipeline pipeline = Pipeline.create(options);

        pipeline
                .apply(every(standardSeconds(1))) //produces a number every second
                .apply(MapElements.into(TypeDescriptor.of(String.class)).via(String::valueOf))
                .apply(fixedWindow(lateness))
                .apply(Distinct.create())
                .apply(delay(10_000L))
                .apply(writeData)
                .getPerDestinationOutputFilenames()

                .apply(MapElements.into(TypeDescriptor.of(String.class)).via(KV::getValue))
                .apply(logWindowInfo("Emitted File"))

                .apply("WindowAfterEmitted", Distinct.create()) //here some files are dropped by LateDataFilter
                .apply(logWindowInfo("File processed"))
                .apply(writeFiles);

        pipeline.run();
    }

    private static PTransform<PBegin, PCollection<Long>> every(Duration interval) {
        return GenerateSequence.from(0).withRate(1, interval);
    }

    private static Window<String> fixedWindow(Duration lateness) {
        return Window.<String>into(FixedWindows.of(standardSeconds(5L)))
                .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY)
                .withAllowedLateness(lateness)
                .discardingFiredPanes();
    }

    private static ParDo.SingleOutput<String, String> logWindowInfo(String message) {
        return ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext context, BoundedWindow boundedWindow) {
                log.info("[lens] [{}] element = {} in window = {}, timestamp = {}, pane = {}, index ={}, timing ={}, isFirst ={}, isLast={}",
                        message,
                        context.element(),
                        boundedWindow,
                        context.timestamp(),
                        context.pane(),
                        context.pane().getIndex(),
                        context.pane().getTiming(),
                        context.pane().isFirst(),
                        context.pane().isLast()
                );
                context.output(context.element());
            }
        });
    }

    private static ParDo.SingleOutput<String, String> delay(Long millis) {
        return ParDo.of(new DoFn<String, String>() {
            @ProcessElement
            public void processElement(ProcessContext context, BoundedWindow boundedWindow) {
                try {
                    Thread.sleep(millis);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                context.output(context.element());
            }
        });
    }

    public static class FilenamePolicy extends FileBasedSink.FilenamePolicy {

        private final String basePath;

        public FilenamePolicy(String basePath) {
            this.basePath = basePath;
        }

        @Override
        public ResourceId windowedFilename(int shardNumber, int numShards, BoundedWindow window, PaneInfo paneInfo, FileBasedSink.OutputFileHints outputFileHints) {
            String location = String.format("/%s-%s-%s-of-%s.txt", window.toString(), paneInfo.getTiming(), shardNumber, numShards);
            return matchNewResource(basePath + location, false);
        }

        @Override
        public ResourceId unwindowedFilename(int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
            throw new UnsupportedOperationException();
        }
    }
}
