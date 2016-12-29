package com.heroku.syncdbs;

import java.util.HashMap;
import java.util.Map;

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
		factory.setUri(System.getenv("RABBITMQ_BIGWIG_URL"));
		connection = factory.newConnection();
		return connection.createChannel();
	}

	public void createConsumer() throws Exception {
		setConsumer(new QueueingConsumer(getChannel()));
		getChannel().basicConsume(getQName(), false, getConsumer());
	}

	public void connect(String qn) throws Exception {
		setChannel(getQChannel());

		Map<String, Object> params = new HashMap<String, Object>();
		params.put("x-ha-policy", "all");
		getChannel().queueDeclare(qn, true, false, false, params);
		getChannel().basicQos(1);

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

	public String getQName() {
		return qName;
	}

	public void close() throws Exception{
		getConnection().close();
	}
	
	public void setQName(String qName) {
		this.qName = qName;
	}

	public QueueingConsumer getConsumer() {
		return consumer;
	}

	private void setConsumer(QueueingConsumer workerConsumer) {
		this.consumer = workerConsumer;
	}

	public Channel getChannel() {
		return channel;
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
