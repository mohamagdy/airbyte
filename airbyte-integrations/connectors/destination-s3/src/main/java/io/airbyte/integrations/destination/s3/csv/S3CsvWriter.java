/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.s3.csv;

import alex.mojaki.s3upload.MultiPartOutputStream;
import alex.mojaki.s3upload.StreamTransferManager;
import com.amazonaws.services.s3.AmazonS3;
import io.airbyte.integrations.destination.s3.S3DestinationConfig;
import io.airbyte.integrations.destination.s3.S3Format;
import io.airbyte.integrations.destination.s3.util.S3StreamTransferManagerHelper;
import io.airbyte.integrations.destination.s3.writer.BaseS3Writer;
import io.airbyte.integrations.destination.s3.writer.S3Writer;
import io.airbyte.protocol.models.AirbyteRecordMessage;
import io.airbyte.protocol.models.ConfiguredAirbyteStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.UUID;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.QuoteMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3CsvWriter extends BaseS3Writer implements S3Writer {

  private static final Logger LOGGER = LoggerFactory.getLogger(S3CsvWriter.class);

  private final CsvSheetGenerator csvSheetGenerator;
  private final StreamTransferManager uploadManager;
  private final MultiPartOutputStream outputStream;
  private final CSVPrinter csvPrinter;
  private final String objectKey;

  public S3CsvWriter(final S3DestinationConfig config,
      final AmazonS3 s3Client,
      final ConfiguredAirbyteStream configuredStream,
      final Timestamp uploadTimestamp,
      final int uploadThreads,
      final int queueCapacity,
      final String customSuffix)
      throws IOException {
    this(config, s3Client, configuredStream, uploadTimestamp, customSuffix);

    this.uploadManager
        .numUploadThreads(uploadThreads)
        .queueCapacity(queueCapacity);
  }

  public S3CsvWriter(final S3DestinationConfig config,
      final AmazonS3 s3Client,
      final ConfiguredAirbyteStream configuredStream,
      final Timestamp uploadTimestamp)
      throws IOException {
    this(config, s3Client, configuredStream, uploadTimestamp, BaseS3Writer.DEFAULT_SUFFIX);
  }

  public S3CsvWriter(final S3DestinationConfig config,
      final AmazonS3 s3Client,
      final ConfiguredAirbyteStream configuredStream,
      final Timestamp uploadTimestamp,
      final String customFileSuffix)
      throws IOException {
    super(config, s3Client, configuredStream);

    final S3CsvFormatConfig formatConfig = (S3CsvFormatConfig) config.getFormatConfig();
    this.csvSheetGenerator = CsvSheetGenerator.Factory.create(configuredStream.getStream().getJsonSchema(),
        formatConfig);

    final String outputFilename = BaseS3Writer.getOutputFilename(uploadTimestamp, customFileSuffix, S3Format.CSV);
    this.objectKey = String.join("/", outputPrefix, outputFilename);

    LOGGER.info("Full S3 path for stream '{}': s3://{}/{}", stream.getName(), config.getBucketName(),
        objectKey);

    this.uploadManager = S3StreamTransferManagerHelper.getDefault(
        config.getBucketName(), objectKey, s3Client, config.getFormatConfig().getPartSize());
    // We only need one output stream as we only have one input stream. This is reasonably performant.
    this.outputStream = uploadManager.getMultiPartOutputStreams().get(0);
    this.csvPrinter = new CSVPrinter(new PrintWriter(outputStream, true, StandardCharsets.UTF_8),
        CSVFormat.DEFAULT.withQuoteMode(QuoteMode.ALL)
            .withHeader(csvSheetGenerator.getHeaderRow().toArray(new String[0])));
  }

  @Override
  public void write(final UUID id, final AirbyteRecordMessage recordMessage) throws IOException {
    csvPrinter.printRecord(csvSheetGenerator.getDataRow(id, recordMessage));
  }

  @Override
  protected void closeWhenSucceed() throws IOException {
    csvPrinter.close();
    outputStream.close();
    uploadManager.complete();
  }

  @Override
  protected void closeWhenFail() throws IOException {
    csvPrinter.close();
    outputStream.close();
    uploadManager.abort();
  }

  @Override
  public String getObjectKey() {
    return objectKey;
  }
}
