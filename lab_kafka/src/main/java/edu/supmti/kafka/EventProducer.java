package edu.supmti.kafka;
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class EventProducer {

    public static void main(String[] args) throws Exception {

        // Vérifier que le topic est fourni comme argument
        if (args.length == 0) {
            System.out.println("Usage : java -jar lab_kafka-1.0-SNAPSHOT-jar-with-dependencies.jar <topicName>");
            return;
        }

        String topicName = args[0]; // lire le nom du topic fourni comme paramètre

        // Configurations du producteur Kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // serveur Kafka
        props.put("acks", "all");                           // acquittement
        props.put("retries", 0);                            // réessais automatiques
        props.put("batch.size", 16384);                     // taille du batch
        props.put("buffer.memory", 33554432);              // mémoire tampon
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Création du producteur Kafka
        Producer<String, String> producer = new KafkaProducer<>(props);

        // Envoi de 10 messages de test
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<>(topicName, Integer.toString(i), Integer.toString(i)));
        }

        System.out.println("Messages envoyés avec succès sur le topic : " + topicName);

        // Fermer le producteur
        producer.close();
    }
}



