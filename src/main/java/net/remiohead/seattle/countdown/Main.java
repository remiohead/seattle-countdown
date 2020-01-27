package net.remiohead.seattle.countdown;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.http.client.fluent.Request;
import org.apache.http.message.BasicNameValuePair;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class Main implements RequestStreamHandler {

    private static final String URL_ENV = "URL";
    private static final String API_KEY_ENV = "API_KEY";
    private static final String GROUP_KEY_ENV = "GROUP_KEY";
    private static final String LINES_ENV = "LINES";

    @Override
    public void handleRequest(
            final InputStream input,
            final OutputStream output,
            final Context context) throws IOException {
        send(message());
    }

    private void send(String message) throws IOException {
        final String response = Request
                .Post(URL_ENV)
                .bodyForm(
                        Lists.newArrayList(
                                new BasicNameValuePair(
                                        "token", API_KEY_ENV),
                                new BasicNameValuePair(
                                        "user", GROUP_KEY_ENV),
                                new BasicNameValuePair(
                                        "message", message),
                                new BasicNameValuePair(
                                        "title", "Seattle Countdown")),
                        StandardCharsets.UTF_8)
                .execute()
                .returnContent()
                .asString(StandardCharsets.UTF_8);
        System.out.println(response);
    }

    private static String message() {
        final DateTimeZone timeZone =
                DateTimeZone.forID(
                        "America/Chicago");
        final DateTime now =
                DateTime.now(timeZone);

        final DateTime then =
                DateTime.parse(
                        System.getenv(
                                "TARGET_DATE"))
                        .toDateTime(timeZone);

        int days = Math.max(0,
                Days.daysBetween(now, then).getDays());

        final List<ImmutableLine> lines = getLines();

        final DynamoDB client =
                new DynamoDB(AmazonDynamoDBClientBuilder
                        .standard()
                        .withRegion(Regions.US_WEST_2)
                        .build());
        final Table t = client.getTable("Seattle");
        final PrimaryKey pkid =
                new PrimaryKey(
                        "Key",
                        0);
        final Item item = t.getItem(pkid);
        final Set<Number> values =
                Sets.newHashSet();
        item.getNumberSet("Values")
                .stream()
                .map(BigDecimal::intValue)
                .forEach(values::add);
        System.out.println("Values="+values);

        if(values.size() == lines.size()) {
            values.clear();
        }

        final int nextItem =
                nextItemIndex(
                        values,
                        lines.size());
        System.out.println("Next item="+nextItem);
        values.add(nextItem);
        item.withNumberSet("Values", values);
        t.putItem(item);

        return lines.get(nextItem)
                .getLine()
                .replace("$", String.valueOf(days));
    }


    private static List<ImmutableLine> getLines() {
        final AtomicInteger count = new AtomicInteger();

        try (final BufferedReader in =
                     new BufferedReader(
                             new InputStreamReader(Request
                                     .Get(LINES_ENV)
                                     .execute()
                                     .returnContent()
                                     .asStream()))) {
            return in
                    .lines()
                    .map(line -> ImmutableLine
                            .builder()
                            .key(count.getAndIncrement())
                            .line(line)
                            .build())
                    .collect(Collectors.toList());

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static int nextItemIndex(
            final Set<Number> values,
            final int max) {
        final SecureRandom r = new SecureRandom();
        int i = r.nextInt(max);
        while(values.contains(i)) {
            i = r.nextInt(max);
        }
        return i;
    }


    public static void main(String[] args) {
        Set<Number> set = new HashSet<>();
        final int max = 38;
        for(int i = 0; i <= max; i++) {
            int next =
                    nextItemIndex(set, max);
            System.out.println(next);
            set.add(next);
        }

    }
}
