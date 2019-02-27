/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package demo;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class App {

    private static final int BATCH_SIZE = 300;
    private static final String INDEX = "trips";

    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost", 9200, "http")));

        boolean indexExists = client.indices().exists(new GetIndexRequest(INDEX), RequestOptions.DEFAULT);
        if (indexExists) {
            // delete existing index
            client.indices().delete(new DeleteIndexRequest(INDEX), RequestOptions.DEFAULT);
        }
        // create new index
        String indexSource = Files.readString(new File(App.class.getClassLoader().getResource("trips_index.json").getFile()).toPath());
        client.indices().create(new CreateIndexRequest("trips").source(indexSource, XContentType.JSON), RequestOptions.DEFAULT);

        int idx = -1; // document ID
        BulkRequest bulkRequest = new BulkRequest();
        List<CompletableFuture<BulkResponse>> responses = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(args[0]))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (idx == -1) {
                    idx+=1;
                    continue;
                }
                String[] values = line.replaceAll("\"", "").split(",");
                bulkRequest.add(new IndexRequest("trips").id(String.valueOf(idx++)).source(
                    "duration_sec", values[0],
                    "start_time", values[1],
                    "end_time", values[2],
                    "start_station_id", values[3],
                    "start_station_name", values[4],
                    "start_station_location", values[5] + "," + values[6],
                    "end_station_id", values[7],
                    "end_station_name", values[8],
                    "end_station_location", values[9] + "," + values[10],
                    "bike_id", values[11],
                    "user_type", values[12],
                    "member_birth_year", values[13].isEmpty() ? null : values[13],
                    "member_gender", values[14].isEmpty() ? null : values[14],
                    "bike_share_for_all_trip", values[15]
                ));
                if (idx % BATCH_SIZE == 0) {
                    responses.add(bulkFuture(client, bulkRequest));
                    bulkRequest = new BulkRequest();
                    System.out.println("progress: indexing " + idx + " documents");
                }
            }
        }
        responses.add(bulkFuture(client, bulkRequest));
        System.out.println("progress: indexing " + idx + " documents");

        // wait for all bulk requests to finish
        CompletableFuture.allOf(responses.toArray(new CompletableFuture[responses.size()])).join();

        System.out.println("done indexing...");
        System.out.println("now for querying what we just indexed...");

        TermsAggregationBuilder termsAggBuilder = AggregationBuilders.terms("by_start_station").field("start_station_name");
        termsAggBuilder.subAggregation(
            AggregationBuilders.dateHistogram("by_2hour").field("start_time").dateHistogramInterval(DateHistogramInterval.hours(2)));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(termsAggBuilder);

        SearchResponse response = client.search(new SearchRequest().indices(INDEX).source(searchSourceBuilder), RequestOptions.DEFAULT);

        // closing RestHighLevelClient closes RestClient
        client.close();
    }

    private static CompletableFuture<BulkResponse> bulkFuture(RestHighLevelClient client, BulkRequest request) {
        CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        client.bulkAsync(request, RequestOptions.DEFAULT,
            ActionListener.wrap(future::complete, future::completeExceptionally));
        return future;
    }
}