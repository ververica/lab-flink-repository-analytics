package com.ververica.platform.io.source;

import static com.ververica.platform.io.source.GithubSource.dateToLocalDateTime;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

import com.ververica.platform.entities.Email;
import java.io.ByteArrayOutputStream;
import java.io.CharConversionException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.james.mime4j.dom.Entity;
import org.apache.james.mime4j.dom.Message;
import org.apache.james.mime4j.dom.MessageBuilder;
import org.apache.james.mime4j.dom.Multipart;
import org.apache.james.mime4j.dom.TextBody;
import org.apache.james.mime4j.dom.address.Mailbox;
import org.apache.james.mime4j.dom.field.FieldName;
import org.apache.james.mime4j.dom.field.MailboxField;
import org.apache.james.mime4j.dom.field.MailboxListField;
import org.apache.james.mime4j.mboxiterator.CharBufferWrapper;
import org.apache.james.mime4j.mboxiterator.MboxIterator;
import org.apache.james.mime4j.message.DefaultMessageBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApacheMboxSource extends RichSourceFunction<Email> implements CheckpointedFunction {

  private static final Logger LOG = LoggerFactory.getLogger(ApacheMboxSource.class);

  private static final boolean SKIP_NON_EXISTING_MBOX = true;

  private static final DateTimeFormatter MBOX_DATE_FORMATTER =
      new DateTimeFormatterBuilder()
          .parseCaseInsensitive()
          .appendValue(YEAR, 4, 4, SignStyle.EXCEEDS_PAD)
          .appendValue(MONTH_OF_YEAR, 2)
          .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
          .toFormatter();

  public static final List<CharsetEncoder> ENCODERS =
      Arrays.asList(
          StandardCharsets.ISO_8859_1.newEncoder(),
          StandardCharsets.UTF_8.newEncoder(),
          StandardCharsets.US_ASCII.newEncoder(),
          StandardCharsets.UTF_16.newEncoder(),
          StandardCharsets.UTF_16BE.newEncoder(),
          StandardCharsets.UTF_16LE.newEncoder());

  private final long pollIntervalMillis;

  private final String listName;

  private LocalDateTime lastDate;

  private volatile boolean running = true;

  private transient ListState<LocalDateTime> state;

  private transient File mboxFile = null;

  public ApacheMboxSource(String listName, LocalDateTime startDate, long pollIntervalMillis) {
    this.listName = listName;
    this.lastDate = startDate;
    this.pollIntervalMillis = pollIntervalMillis;
  }

  @Override
  public void run(SourceContext<Email> ctx) throws IOException {
    LocalDateTime nextPollTime = LocalDateTime.MIN;
    while (running) {
      if (nextPollTime.isAfter(LocalDateTime.now())) {
        try {
          //noinspection BusyWait
          Thread.sleep(10_000);
        } catch (InterruptedException e) {
          running = false;
        }
        continue;
      }

      String url =
          "http://mail-archives.apache.org/mod_mbox/"
              + listName
              + "/"
              + MBOX_DATE_FORMATTER.format(lastDate)
              + ".mbox";
      LOG.info("Fetching mails from {}", url);

      List<Email> emails = null;
      try {
        InputStream in = new URL(url).openStream();
        Files.copy(in, mboxFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
      } catch (FileNotFoundException e) {
        if (SKIP_NON_EXISTING_MBOX) {
          emails = Collections.emptyList();
        } else {
          throw e;
        }
      }

      if (emails == null) {
        for (CharsetEncoder encoder : ENCODERS) {
          encoder.reset();
          try (MboxIterator mboxIterator =
              MboxIterator.fromFile(mboxFile)
                  .maxMessageSize(20 * 1024 * 1024)
                  .charset(encoder.charset())
                  .build()) {
            LOG.info("Decoding {} with {}", url, encoder);

            emails =
                StreamSupport.stream(mboxIterator.spliterator(), false)
                    .map(message -> fromMessage(message, encoder.charset()))
                    .filter(email -> email.getDate().isAfter(lastDate))
                    .collect(Collectors.toList());

            LOG.info("Found {} messages in {}", emails.size(), url);
            break;
          } catch (CharConversionException | IllegalArgumentException ex) {
            // Not the right encoder
          } catch (IOException ex) {
            LOG.warn("Failed to open mbox file {} downloaded from {}", mboxFile.getName(), url, ex);
          }
        }

        if (emails == null) {
          throw new IOException("No valid charset found");
        }
      }

      boolean isCurrentMonth =
          lastDate
              .withDayOfMonth(1)
              .truncatedTo(ChronoUnit.DAYS)
              .isEqual(LocalDateTime.now().withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS));
      long maxTimestamp = 0L;
      LocalDateTime maxDate = null;
      synchronized (ctx.getCheckpointLock()) {
        for (Email email : emails) {
          long timestamp =
              email.getDate().atZone(GithubSource.EVALUATION_ZONE).toInstant().toEpochMilli();
          ctx.collectWithTimestamp(email, timestamp);
          if (timestamp > maxTimestamp) {
            maxTimestamp = timestamp;
            maxDate = email.getDate();
          }
        }

        // If current month is reached, delay next poll for data.
        // Also update next date to allow according with the highest seen (current month) or
        // assumed (for previous months).
        final LocalDateTime nextDate;
        if (isCurrentMonth) {
          nextPollTime = LocalDateTime.now().plus(pollIntervalMillis, ChronoUnit.MILLIS);
          if (maxDate == null) {
            nextDate = lastDate;
          } else {
            nextDate = maxDate;
          }
        } else {
          // assume month is complete
          nextDate =
              lastDate.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS).plus(1, ChronoUnit.MONTHS);
          nextPollTime = nextDate;
        }

        lastDate = nextDate;
        long nextDateMillis =
            nextDate.atZone(GithubSource.EVALUATION_ZONE).toInstant().toEpochMilli();
        ctx.emitWatermark(new Watermark(nextDateMillis - 1));
      }
    }
  }

  private static Email fromMessage(CharBufferWrapper input, Charset charset) {
    try (InputStream in = input.asInputStream(charset)) {
      MessageBuilder builder = new DefaultMessageBuilder();
      Message message = builder.parseMessage(in);

      LocalDateTime date = dateToLocalDateTime(message.getDate());

      Body body = new Body();
      if (message.isMultipart()) {
        parseBodyParts((Multipart) message.getBody(), body);
      } else {
        parsePlainOrHtmlBody(message, body);
      }

      Tuple2<String, Mailbox> author = getAuthor(message);
      return Email.builder()
          .date(date)
          .fromRaw(author.f0)
          .fromEmail(author.f1.toString())
          .subject(message.getSubject())
          .textBody(body.getTextBody())
          .htmlBody(body.getHtmlBody())
          .build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse email", e);
    }
  }

  private static class Body {
    StringBuilder textBody = new StringBuilder();
    StringBuilder htmlBody = new StringBuilder();

    String getTextBody() {
      return textBody.length() == 0 ? null : textBody.toString();
    }

    String getHtmlBody() {
      return htmlBody.length() == 0 ? null : htmlBody.toString();
    }
  }

  private static void parseBodyParts(Multipart multipart, Body body) throws IOException {
    for (Entity part : multipart.getBodyParts()) {
      if (part.getDispositionType() != null
          && !part.getDispositionType().equals("")
          && !part.getDispositionType().equals("inline")) {
        // attachment, ignore
        LOG.trace(
            "Ignoring part: {}, {}, {}",
            part.getDispositionType(),
            part.getMimeType(),
            part.getFilename());
      } else {
        parsePlainOrHtmlBody(part, body);
      }

      // If current part contains other, parse it again by recursion
      if (part.isMultipart()) {
        parseBodyParts((Multipart) part.getBody(), body);
      }
    }
  }

  private static void parsePlainOrHtmlBody(Entity entity, Body body) throws IOException {
    if (entity.getMimeType().equals("text/plain")) {
      String txt = parseTextPart(entity);
      body.textBody.append(txt);
    } else if (entity.getMimeType().equals("text/html")) {
      String html = parseTextPart(entity);
      body.htmlBody.append(html);
    }
  }

  private static String parseTextPart(Entity part) throws IOException {
    TextBody tb = (TextBody) part.getBody();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    tb.writeTo(baos);
    return baos.toString();
  }

  private static Tuple2<String, Mailbox> getAuthor(Message message) {
    MailboxListField fromField = (MailboxListField) message.getHeader().getField(FieldName.FROM);
    if (fromField == null || fromField.getMailboxList().isEmpty()) {
      MailboxField senderField = (MailboxField) message.getHeader().getField(FieldName.SENDER);
      return getAuthor(senderField);
    } else {
      Mailbox from = fromField.getMailboxList().get(0);
      return Tuple2.of(fromField.getBody(), from);
    }
  }

  private static Tuple2<String, Mailbox> getAuthor(MailboxField sender) {
    if (sender != null) {
      return Tuple2.of(sender.getBody(), sender.getMailbox());
    } else {
      return Tuple2.of("unknown", null);
    }
  }

  @Override
  public void cancel() {
    running = false;
  }

  @Override
  public void open(Configuration configuration) throws IOException {
    mboxFile = File.createTempFile("temp", null);
  }

  @Override
  public void close() throws Exception {
    if (mboxFile != null) {
      if (!mboxFile.delete()) {
        throw new IOException("Failed to delete file " + mboxFile);
      }
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
    state.clear();
    state.add(lastDate);
  }

  @Override
  public void initializeState(FunctionInitializationContext ctx) throws Exception {
    state =
        ctx.getOperatorStateStore()
            .getListState(new ListStateDescriptor<>("lastDate", LocalDateTime.class));

    if (ctx.isRestored()) {
      Iterator<LocalDateTime> data = state.get().iterator();
      if (data.hasNext()) {
        lastDate = data.next();
      }
    }
  }
}
