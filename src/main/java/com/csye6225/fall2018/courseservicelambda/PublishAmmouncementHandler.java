package com.csye6225.fall2018.courseservicelambda;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Index;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;
import com.amazonaws.services.sns.AmazonSNS;
import com.amazonaws.services.sns.AmazonSNSClientBuilder;

public class PublishAmmouncementHandler implements RequestHandler<DynamodbEvent, Integer>
{
    @Override
    public Integer handleRequest(DynamodbEvent event, Context context)
    {
        final DynamoDB dynamoDB = getDynamoDBClient();
        final AmazonSNS snsClient = getSNSClient();

        context.getLogger().log("Received event: " + event);

        for (final DynamodbStreamRecord record : event.getRecords())
        {
            context.getLogger().log(record.getEventID());
            context.getLogger().log(record.getEventName());
            context.getLogger().log(record.getDynamodb().toString());
            final Map<String, AttributeValue> item = record.getDynamodb().getNewImage();
            if (item == null || item.get("announcementText") == null || item.get("boardId") == null)
            {
                continue;
            }

            final String notificationTopic = getNotificationTopic(dynamoDB, item.get("boardId").getS());
            if (notificationTopic == null)
            {
                continue;
            }
            snsClient.publish(notificationTopic, item.get("announcementText").getS());
        }
        return event.getRecords().size();
    }

    private String getNotificationTopic(final DynamoDB dynamoDB, final String boardId)
    {
        final Index boardIndex = dynamoDB.getTable("board").getIndex("boardId");
        final ItemCollection<QueryOutcome> boardItems = boardIndex.query(new QuerySpec()
                .withKeyConditionExpression("boardId = :v1").withValueMap(new ValueMap().withString(":v1", boardId)));
        if (!boardItems.iterator().hasNext())
        {
            return null;
        }
        Object courseId = boardItems.iterator().next().get("courseId");
        if (courseId == null)
        {
            return null;
        }

        final Index courseIndex = dynamoDB.getTable("course").getIndex("courseId");
        final ItemCollection<QueryOutcome> courseItems = courseIndex
                .query(new QuerySpec().withKeyConditionExpression("courseId = :v1")
                        .withValueMap(new ValueMap().withString(":v1", courseId.toString())));
        if (!courseItems.iterator().hasNext())
        {
            return null;
        }
        Object notificationTopic = courseItems.iterator().next().get("notificationTopic");
        if (notificationTopic == null)
        {
            return null;
        }
        return notificationTopic.toString();
    }

    private DynamoDB getDynamoDBClient()
    {
        return new DynamoDB(AmazonDynamoDBClientBuilder.standard().withRegion("us-west-2").build());
    }

    private AmazonSNS getSNSClient()
    {
        return AmazonSNSClientBuilder.standard().withRegion("us-west-2").build();
    }
}