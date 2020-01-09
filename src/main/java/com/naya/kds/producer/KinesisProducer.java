package com.naya.kds.producer;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.naya.avro.EventAttributes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class KinesisProducer {

    private static String streamName = "dev-events";


    public static void main(String[] args) throws InterruptedException, JsonMappingException {

        AmazonKinesis kinesisClient = getAmazonKinesisClient("us-east-1");

        try {
            sendData(kinesisClient, streamName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static AmazonKinesis getAmazonKinesisClient(String regionName) {

        AmazonKinesisClientBuilder clientBuilder = AmazonKinesisClientBuilder.standard();
        clientBuilder.setEndpointConfiguration(
                new AwsClientBuilder.EndpointConfiguration("kinesis.us-east-1.amazonaws.com",
                        regionName));
        clientBuilder.withCredentials(DefaultAWSCredentialsProviderChain.getInstance());
        clientBuilder.setClientConfiguration(new ClientConfiguration());

        return clientBuilder.build();
    }

    private static void sendData(AmazonKinesis kinesisClient, String streamName) throws IOException {

        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();

        putRecordsRequest.setStreamName(streamName);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
            EventAttributes eventAttributes = EventAttributes.newBuilder().setName("Jon.Doe").build();
            putRecordsRequestEntry.setData(eventAttributes.toByteBuffer());
            putRecordsRequestEntry.setPartitionKey(String.format("partitionKey-%d", i));
            putRecordsRequestEntryList.add(putRecordsRequestEntry);
        }

        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        PutRecordsResult putRecordsResult = kinesisClient.putRecords(putRecordsRequest);
        System.out.println("Put Result" + putRecordsResult);
    }


}
