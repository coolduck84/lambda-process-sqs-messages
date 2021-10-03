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

public class Handler implements RequestHandler<SQSEvent, String> {

	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public String handleRequest(SQSEvent event, Context context) {
		LambdaLogger logger = context.getLogger();
		logger.log("\nEvent => " + gson.toJson(event));
		logger.log("\nContext => " + gson.toJson(context));

		AWSStepFunctions sfnClient = AWSStepFunctionsClientBuilder.standard().withRegion(Regions.US_EAST_1).build();

		String input = null;
		for (SQSMessage msg : event.getRecords()) {
			input = msg.getBody();
			logger.log("\nInput: " + input);
		}

		try {
			StartExecutionRequest request = new StartExecutionRequest()
					.withStateMachineArn(System.getenv("stepFunctionARN"))
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
		
		logger.log("\n");
		return "Processed successfully";
	}
}
