package com.heroku.syncdbs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.QueueingConsumer.Delivery;

public class QueueManager {
	private Channel channel = null;
	private QueueingConsumer consumer = null;
	private String qName;
	private Connection connection = null;
	
	public QueueManager() {
		super();
	}

	public QueueManager(String qName) {
		super();
		this.setQName(qName);
	}

	private Channel getQChannel() throws Exception {
		ConnectionFactory factory = new ConnectionFactory();
		factory.setUri(Settings.getQueueUrl());
		connection = factory.newConnection();
		return connection.createChannel();
	}

	public void createConsumer() throws Exception {
		setConsumer(new QueueingConsumer(getChannel()));
		getChannel().basicConsume(getQName(), false, getConsumer());
	}

	public void connect(String qn) throws Exception {
		setChannel(getQChannel());
		setQName(qn);
		
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("x-ha-policy", "all");
		getChannel().queueDeclare(qn, true, false, false, params);
		getChannel().basicQos(1);
		System.out.println("Listening for messages on queue [" + qn + "]");
	}

	public void ack(Delivery delivery) throws Exception {
		getChannel().basicAck(delivery.getEnvelope().getDeliveryTag(), false);
	}

	public Delivery nextDelivery() throws Exception {

		if (getConsumer() == null)
			createConsumer();

		return getConsumer().nextDelivery();
	}

	public void sendMessage(JobMessage jm) throws Exception {
		channel.basicPublish("", getQName(), MessageProperties.PERSISTENT_TEXT_PLAIN,
				jm.toJson().getBytes("UTF-8"));
	}

	public JobMessage parseJsonMessage(byte[] body) throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		return mapper.readValue(body, JobMessage.class);
	}

	public String getQName() {
		return this.qName;
	}

	public void close() throws Exception{
		getConnection().close();
	}
	
	public void setQName(String qName) {
		this.qName = qName;
	}

	public QueueingConsumer getConsumer() {
		return this.consumer;
	}

	private void setConsumer(QueueingConsumer workerConsumer) {
		this.consumer = workerConsumer;
	}

	public Channel getChannel() {
		return this.channel;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}


}
