package com.madhouse.dsp.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class MyParquetWriters {
    public static ParquetWriterFactory<GenericRecord> forGenericRecord(Schema schema) {
        final String schemaString = schema.toString();
        final ParquetBuilder<GenericRecord> builder = (out) -> createAvroParquetWriter(schemaString, GenericData.get(), out);
        return new ParquetWriterFactory<>(builder);
    }
    private static <T> ParquetWriter<T> createAvroParquetWriter(
            String schemaString,
            GenericData dataModel,
            OutputFile out) throws IOException {

        final Schema schema = new Schema.Parser().parse(schemaString);

        return AvroParquetWriter.<T>builder((Path)out)
                .withSchema(schema)
                .withDataModel(dataModel)
                .build();
    }
}
