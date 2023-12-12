package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.Properties;
import java.util.Scanner;

//Create java class named “SimpleProducer”
public class SimpleProducer {

    public static void main(String[] args) throws Exception{

        // Check arguments length value
      /*  if(args.length == 0){
            System.out.println("Enter topic name");
            return;
        }
*/
        //Assign topicName to string variable
        System.out.println("Enter topic name");
        Scanner sn = new Scanner(System.in);
        String topicName = sn.next();

        // create instance for properties to access producer configs
        Properties props = new Properties();

        //Assign localhost id
        props.put("bootstrap.servers", "localhost:9093");

        //Set acknowledgements for producer requests.
        props.put("acks", "all");

                //If the request fails, the producer can automatically retry,
                props.put("retries", 0);

        //Specify buffer size in config
        props.put("batch.size", 16384);

        //Reduce the no of requests less than 0
        props.put("linger.ms", 1);

        //The buffer.memory controls the total amount of memory available to the producer for buffering.
        props.put("buffer.memory", 33554432);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer", ByteArraySerializer.class.getName());

        Producer<String, byte[]> producer = new KafkaProducer<>(props);

        int i=100;
        Employee employee;
        while(1==1) {

            employee=new Employee("E00"+i, sn.next());
            byte[] employee1ByteArray = Serializer.serialize(employee);
            producer.send(new ProducerRecord<String, byte[]>(topicName,
                    Integer.toString(i++), employee1ByteArray));
        }
        //System.out.println("Message sent successfully");

        //producer.close();
    }
}