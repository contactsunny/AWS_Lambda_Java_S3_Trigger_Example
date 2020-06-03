package com.contactsunny.poc.s3_lambda_example;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;

import java.util.UUID;

public class App implements RequestHandler<S3Event, String> {
    private DynamoDB dynamoDb;

    @Override
    public String handleRequest(S3Event s3Event, Context context) {
        initDynamoDbClient();

        S3EventNotification.S3EventNotificationRecord record = s3Event.getRecords().get(0);
        String bucket = record.getS3().getBucket().getName();
        String key = record.getS3().getObject().getUrlDecodedKey();

        System.out.println("Bucket: " + bucket);
        System.out.println("Key: " + key);

        putRecordToDynamoDB(bucket, key);

        return null;
    }

    private void putRecordToDynamoDB(String bucket, String key) {
        Item item = new Item().withString("id", String.valueOf(UUID.randomUUID()))
                .withString("key", key)
                .withString("bucket", bucket);

        System.out.println("Adding item to DynamoDB: " + item.toJSONPretty());

        dynamoDb.getTable("s3_files").putItem(item);
    }


    private void initDynamoDbClient() {
        AmazonDynamoDB client = AmazonDynamoDBClient.builder()
                .withRegion(Regions.US_EAST_1).build();

        dynamoDb = new DynamoDB(client);
    }
}
