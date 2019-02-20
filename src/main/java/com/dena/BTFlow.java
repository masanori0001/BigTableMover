package com.dena;

import com.google.bigtable.v2.Mutation;
import com.google.bigtable.v2.Row;
import com.google.cloud.bigtable.config.BigtableOptions;
import com.google.protobuf.ByteString;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigtable.BigtableIO;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.hbase.client.Put;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BTFlow {

    private static final Logger LOG = LoggerFactory.getLogger(BTFlow.class);

    public interface BTFlowOptions extends DataflowPipelineOptions {
        @Description("debug mode.")
        @Default.Boolean(false)
        Boolean isDebug();
        void setDebug(Boolean value);

        @Description("Project ID")
        @Required
        String getProjectID();
        void setProjectID(String value);

        @Description("Input Table")
        @Required
        String getInputTable();
        void setInputTable(String value);

        @Description("Instance")
        @Required
        String getInstance();
        void setInstance(String value);

        @Description("Output Table")
        @Required
        String getOutputTable();
        void setOutputTable(String value);
    }

    public static void main(String[] args){
        BTFlowOptions options = PipelineOptionsFactory.fromArgs(args)
                .withValidation().as(BTFlowOptions.class);

        options.setJobName("bigtable-mover");

        LOG.info("Options: " + options);

        Pipeline p;
        PCollection<Row> source;
        p = Pipeline.create(options);

        source = p.apply("read",
                BigtableIO.read()
                        .withProjectId(options.getProjectID())
                        .withInstanceId(options.getInstance())
                        .withTableId(options.getInputTable()));
        PCollection<KV<ByteString, Iterable<Mutation>>> result = source.apply(ParDo.of(MUTATION_TRANSFORM));

        result.apply("write",
                BigtableIO.write()
                        .withProjectId(options.getProjectID())
                        .withInstanceId(options.getInstance())
                        .withTableId(options.getOutputTable()));


        p.run().waitUntilFinish();
    }

    private static final DoFn<Row, KV<ByteString, Iterable<Mutation>>> MUTATION_TRANSFORM =
            new DoFn<Row, KV<ByteString, Iterable<Mutation>>>() {
                private static final long serialVersionUID = 1L;

                @ProcessElement
                public void processElement(DoFn<Row, KV<ByteString, Iterable<Mutation>>>.ProcessContext c)
                        throws Exception {
                    Row element = c.element();
                    ByteString key = element.getKey();
                    List<Mutation> mutations = element.getFamiliesList().stream().flatMap(f -> {
                        String familyName = f.getName();
                        return f.getColumnsList().stream().flatMap(col -> {
                            ByteString qualifiler = col.getQualifier();
                            return col.getCellsList().stream().map(cell -> {
                                return addCell(familyName, qualifiler, cell.getValue());
                            });
                        });
                    }).collect(Collectors.toList());

                    c.output(KV.of(key, mutations));
                }
            };

    private static Mutation addCell(String familyName, ByteString qualifier, ByteString value) {


        return Mutation.newBuilder()
                        .setSetCell(
                                Mutation.SetCell.newBuilder()
                                        .setValue(value)
                                        .setFamilyName(familyName)
                                        .setColumnQualifier(qualifier)
                        ).build();
    }
}
