package com.pc.lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSEvent.SQSMessage;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.pc.lambda.model.LambdaDemoIO;

public class Handler implements RequestHandler<SQSEvent, LambdaDemoIO> {

	private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

	public LambdaDemoIO handleRequest(SQSEvent event, Context context) {

		LambdaLogger logger = context.getLogger();
		logger.log("event => " + gson.toJson(event) + "\n");
		logger.log("context => " + gson.toJson(context) + "\n");

		LambdaDemoIO response = new LambdaDemoIO();
		for (SQSMessage msg : event.getRecords()) {
			logger.log("msg.getBody() => " + new String(msg.getBody()) + "\n");
			response = gson.fromJson(msg.getBody(), LambdaDemoIO.class);
		}
		
		logger.log("response => " + gson.toJson(response) + "\n");

		return response;
	}
}
