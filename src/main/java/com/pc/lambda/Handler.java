package com.pc.lambda;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.amazonaws.services.stepfunctions.model.StartExecutionResult;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.pc.lambda.model.LambdaDemoIO;

public class Handler implements RequestHandler<SQSEvent, LambdaDemoIO> {

	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public LambdaDemoIO handleRequest(SQSEvent event, Context context) {

		LambdaLogger logger = context.getLogger();
		logger.log("event => " + gson.toJson(event) + "\n");
		logger.log("context => " + gson.toJson(context) + "\n");

		Regions region = Regions.US_EAST_1;
		AWSStepFunctions sfnClient = AWSStepFunctionsClientBuilder.standard()
				// .withCredentials(credentialsProvider)
				.withRegion(region).build();

		LambdaDemoIO response = new LambdaDemoIO();
		String input = null;
		for (SQSMessage msg : event.getRecords()) {
			input = msg.getBody();
			logger.log("input: " + input + "\n");
			response = gson.fromJson(input, LambdaDemoIO.class);
		}

		logger.log("===========================================\n");
		logger.log("Getting Started with Amazon Step Functions\n");
		logger.log("===========================================\n");

		try {
			StartExecutionRequest request = new StartExecutionRequest()
					.withStateMachineArn("arn:aws:states:us-east-1:052843378853:stateMachine:MyStateMachine")
					.withInput(input);

			logger.log("Starting Execution....\n");
			StartExecutionResult result = sfnClient.startExecution(request);

			logger.log("Request ID: " + result.getSdkResponseMetadata().getRequestId() + "\n");
		} catch (AmazonServiceException ase) {
			logger.log("Caught an AmazonServiceException, which means"
					+ " your request made it to Amazon Step Functions, but was"
					+ " rejected with an error response for some reason.");
			logger.log("Error Message:    " + ase.getMessage());
			logger.log("HTTP Status Code: " + ase.getStatusCode());
			logger.log("AWS Error Code:   " + ase.getErrorCode());
			logger.log("Error Type:       " + ase.getErrorType());
			logger.log("Request ID:       " + ase.getRequestId());
		} catch (AmazonClientException ace) {
			logger.log("Caught an AmazonClientException, which means "
					+ "the client encountered a serious internal problem while "
					+ "trying to communicate with Step Functions, such as not " + "being able to access the network.");
			logger.log("Error Message: " + ace.getMessage());
		}
		
		logger.log("===========================================\n");
		logger.log("Execution Ended for Amazon Step Functions\n");
		logger.log("===========================================\n");

		return response;
	}
}
