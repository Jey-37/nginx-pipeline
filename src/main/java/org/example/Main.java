package org.example;

import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.pubsub.v1.SubscriptionName;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Grok;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Map;

/**
 * The below environmental variables are required:<br/>
 * <b>GOOGLE_APPLICATION_CREDENTIALS:</b> Path to a service account key in JSON format
 */
public class Main
{
    private static final Grok grok;

    static {
        try {
            GrokCompiler grokCompiler = GrokCompiler.newInstance();
            grokCompiler.registerDefaultPatterns();
            grokCompiler.register(Main.class.getClassLoader().getResourceAsStream("grok_pattern"));
            grok = grokCompiler.compile("%{NGINXACCESSLOG}");
        } catch (IOException ex) {
            throw new RuntimeException("Patterns compilation error");
        }
    }

    /**
     * @param args command line arguments for pipeline options<br/>
     * <b>Required arguments are:</b><br/>
     * <b>--project:</b> project name<br/>
     * <b>--inputSubscription:</b> short name of the Cloud Pub/Sub subscription to read from<br/>
     * <b>--dataset:</b> name of the BigQuery dataset<br/>
     * <b>--table:</b> name of the table in the dataset<br/>
     * <br/>
     * <b>To use Dataflow Runner the below arguments are also required:</b><br/>
     * <b>--runner:</b> DataflowRunner<br/>
     * <b>--region:</b> Dataflow regional endpoint<br/>
     * <b>--gcpTempLocation:</b> GCP location for Dataflow to download temporary files
     */
    public static void main(String[] args) throws Exception {
        PipelineOptionsFactory.register(PubSubToBQOptions.class);
        PubSubToBQOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PubSubToBQOptions.class);

        SubscriptionName subName = SubscriptionName.of(
                options.getProject(), options.getInputSubscription());
        options.setInputSubscription(subName.toString());

        run(options).waitUntilFinish();
    }

    public static PipelineResult run(PubSubToBQOptions options) throws Exception {
        Pipeline pipeline = Pipeline.create(options);

        TableReference table = new TableReference()
                .setDatasetId(options.getProject())
                .setDatasetId(options.getDataset())
                .setTableId(options.getTable());

        URI schemaURI = Main.class.getClassLoader().getResource("schema.json").toURI();
        String jsonSchema = new String(Files.readAllBytes(Path.of(schemaURI)));

        pipeline
                .apply("Read PubSub Messages",
                        PubsubIO.readStrings()
                                .fromSubscription(options.getInputSubscription()))
                .apply("Parse messages", ParDo.of(new ParsingFn()))
                .apply("Write Messages to BigQuery",
                        BigQueryIO.writeTableRows().to(table)
                                .withJsonSchema(jsonSchema)
                                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

        return pipeline.run();
    }

    public static class ParsingFn extends DoFn<String, TableRow> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            String logLine = c.element();
            Map<String, Object> parsedLog = grok.match(logLine).capture();

            DateTimeFormatter dtFormatter =
                    DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);
            OffsetDateTime timeLocal = OffsetDateTime.from(
                    dtFormatter.parse(parsedLog.get("time_local").toString()));

            TableRow row = new TableRow()
                    .set("remote_addr", parsedLog.get("remote_addr"))
                    .set("remote_user", parsedLog.get("remote_user"))
                    .set("time_local", timeLocal)
                    .set("request", parsedLog.get("request"))
                    .set("status", parsedLog.get("status"))
                    .set("body_bytes_sent", parsedLog.get("body_bytes_sent"))
                    .set("http_referer", parsedLog.get("http_referer"))
                    .set("http_user_agent", parsedLog.get("http_user_agent"));

            c.output(row);
        }
    }

    public interface PubSubToBQOptions extends GcpOptions {
        @Description("The Cloud Pub/Sub subscription to read from.")
        @Validation.Required
        String getInputSubscription();

        void setInputSubscription(String value);

        @Description("The BigQuery dataset to write to.")
        @Validation.Required
        String getDataset();

        void setDataset(String value);

        @Description("The BigQuery table name to write to.")
        @Validation.Required
        @Default.String("nginx_logs")
        String getTable();

        void setTable(String value);
    }
}