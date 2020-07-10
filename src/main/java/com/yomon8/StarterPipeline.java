/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yomon8;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.reflect.ReflectData;

/**
 * A starter example for writing Beam programs.
 *
 * <p>
 * The example takes two strings, converts them to their upper-case
 * representation and logs them.
 *
 * <p>
 * To run this starter example locally using DirectRunner, just execute it
 * without any additional parameters from your favorite development environment.
 *
 * <p>
 * To run this starter example using managed resource in Google Cloud Platform,
 * you should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE> --runner=DataflowRunner
 */
public class StarterPipeline {

  static class PC {
    String name;
    int cpuCoreCount;
    double memoryGB;
    double diskGB;
    OS os;

    PC(String name, int cpuCoreCount, double memoryGB, double diskGB, OS os) {
      this.name = name;
      this.cpuCoreCount = cpuCoreCount;
      this.memoryGB = memoryGB;
      this.diskGB = diskGB;
      this.os = os;
    }
  }

  static class OS {
    String name;
    String version;

    OS(String name, String version) {
      this.name = name;
      this.version = version;
    }
  }

  private static List<GenericRecord> generateGenericRecords() {

    // Create Sample Data
    List<PC> pcList = Arrays.asList(new PC("HomePC1", 4, 16.0, 128.0, new OS("macOS", "Catalina")),
        new PC("HomePC2", 8, 8.0, 128.0, new OS("Windows", "10")),
        new PC("OfficePC", 8, 16.0, 512.0, new OS("Windows", "10")));

    // Convert from POJOs to GenericRecords
    Schema pcSchema = ReflectData.get().getSchema(PC.class);
    Schema osSchema = ReflectData.get().getSchema(OS.class);
    ArrayList<GenericRecord> list = new ArrayList<>();
    pcList.forEach(p -> {
      Record osRecord = new Record(osSchema);
      osRecord.put("name", p.os.name);
      osRecord.put("version", p.os.version);

      Record pcRecord = new Record(pcSchema);
      pcRecord.put("name", p.name);
      pcRecord.put("cpuCoreCount", p.cpuCoreCount);
      pcRecord.put("memoryGB", p.memoryGB);
      pcRecord.put("diskGB", p.diskGB);
      pcRecord.put("os", osRecord);
      list.add(pcRecord);
    });
    return list;
  }

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();

    // ローカルへの出力用
    String outputPath = "./tmp/parquet";

    // GCSへの出力用
    // String outputPath = "gs://your-bucket-name/parquet";

    // AWS S3への出力用(AWSは最低限Regoinの指定が必要)
    // String outputPath = "s3://your-bucket-name/parquet";
    // options.as(AwsOptions.class).setAwsRegion("ap-northeast-1");

    // GenericRecordとしてデータを取得
    List<GenericRecord> records = generateGenericRecords();

    // スキーマをClassから読み込み変換処理を実行
    Schema pcSchema = ReflectData.get().getSchema(PC.class);
    Pipeline p = Pipeline.create(options);
    p.apply(Create.of(records).withCoder(AvroCoder.of(pcSchema)))
        .apply(FileIO.<GenericRecord>write()
            .via(ParquetIO.sink(pcSchema).withCompressionCodec(CompressionCodecName.SNAPPY)).to(outputPath)
            .withSuffix(".parquet"));

    p.run().waitUntilFinish();
  }
}
