// Package mqtt is the inbound delivery adapter: subscribes to the
// simulator's PV/OP stream and hands each parsed sample to a callback
// (usecase.Ingestor.HandleSample in practice, but this package doesn't
// know that -- it only knows domain.Sample).
package mqtt

import (
	"encoding/json"
	"log"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/maulanaiskak/valve-stiction-ingestion/domain"
)

// Subscribe connects to brokerURL and subscribes to topic, calling onSample
// for every successfully parsed message. Blocks until the connection
// fails to establish; runs the subscription in the background otherwise.
func Subscribe(brokerURL, topic string, onSample func(domain.Sample)) error {
	opts := paho.NewClientOptions().AddBroker(brokerURL).SetClientID("ingestion-service")
	opts.SetOnConnectHandler(func(c paho.Client) {
		log.Printf("connected to MQTT broker %s, subscribing to %s", brokerURL, topic)
		token := c.Subscribe(topic, 1, func(_ paho.Client, msg paho.Message) {
			var s domain.Sample
			if err := json.Unmarshal(msg.Payload(), &s); err != nil {
				log.Printf("failed to parse message: %v", err)
				return
			}
			onSample(s)
		})
		token.Wait()
	})

	client := paho.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}
