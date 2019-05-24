package springgcpsub;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.gcp.pubsub.core.PubSubTemplate;
import org.springframework.cloud.gcp.pubsub.integration.AckMode;
import org.springframework.cloud.gcp.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import org.springframework.cloud.gcp.pubsub.support.GcpPubSubHeaders;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import com.google.cloud.pubsub.v1.AckReplyConsumer;

@SpringBootApplication
public class SpringGcpSubApplication {

	  private static final Log LOGGER = LogFactory.getLog(SpringGcpSubApplication.class);

	  public static void main(String[] args) throws IOException {
	    SpringApplication.run(SpringGcpSubApplication.class, args);
	  }

	  // Inbound channel adapter.

	  // tag::subInputChannel[]
	  @Bean
	  public MessageChannel subInputChannel() {
	    return new DirectChannel();
	  }
	  // end::subInputChannel[]

	  // tag::messageChannelAdapter[]
	  @Bean
	  public PubSubInboundChannelAdapter messageChannelAdapter(
	      @Qualifier("subInputChannel") MessageChannel inputChannel,
	      PubSubTemplate subTemplate) {
	    PubSubInboundChannelAdapter adapter =
	        new PubSubInboundChannelAdapter(subTemplate, "led-control");
	    adapter.setOutputChannel(inputChannel);
	    adapter.setAckMode(AckMode.MANUAL);

	    return adapter;
	  }
	  // end::messageChannelAdapter[]

	  // tag::messageReceiver[]
	  @Bean
	  @ServiceActivator(inputChannel = "subInputChannel")
	  public MessageHandler messageReceiver() {
	    return message -> {
	      LOGGER.info("SpringGcpSub - Message arrived! Payload: " + new String((byte[]) message.getPayload()));
	      AckReplyConsumer consumer =
	          (AckReplyConsumer) message.getHeaders().get(GcpPubSubHeaders.ACKNOWLEDGEMENT);
	      consumer.ack();
	    };
	  }
	  // end::messageReceiver[]

	  // Outbound channel adapter
	  /*
	  // tag::messageSender[]
	  @Bean
	  @ServiceActivator(inputChannel = "pubsubOutputChannel")
	  public MessageHandler messageSender(PubSubTemplate pubsubTemplate) {
	    return (MessageHandler) new PubSubMessageHandler(pubsubTemplate, "led-action");
	  }
	  // end::messageSender[]

	  // tag::messageGateway[]
	  @MessagingGateway(defaultRequestChannel = "pubsubOutputChannel")
	  public interface PubsubOutboundGateway {

	    void sendToPubsub(String text);
	  }
	  // end::messageGateway[]
      */
}
