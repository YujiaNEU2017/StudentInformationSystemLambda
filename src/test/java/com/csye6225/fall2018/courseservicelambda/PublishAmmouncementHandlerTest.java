package com.csye6225.fall2018.courseservicelambda;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;

/**
 * A simple test harness for locally invoking your Lambda function handler.
 */
public class PublishAmmouncementHandlerTest {

    private DynamodbEvent event;

    @Before
    public void createInput() throws IOException {
        // TODO: set up your sample input object here.
        event = TestUtils.parse("/dynamodb-update-event.json", DynamodbEvent.class);
    }

    private Context createContext() {
        TestContext ctx = new TestContext();

        // TODO: customize your context here if needed.
        ctx.setFunctionName("Your Function Name");

        return ctx;
    }

    @Test
    public void testPublishAmmouncementHandler() {
        PublishAmmouncementHandler handler = new PublishAmmouncementHandler();
        Context ctx = createContext();

        Integer output = handler.handleRequest(event, ctx);

        // TODO: validate output here if needed.
        Assert.assertEquals(3, output.intValue());
    }
}
