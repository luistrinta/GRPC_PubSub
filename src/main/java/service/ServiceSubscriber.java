package service;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ServiceSubscriber {

    private static final Logger logger = Logger.getLogger(ServiceSubscriber.class.getName());
    private final ProjectGrpc.ProjectBlockingStub blockingStub;
    private final ProjectGrpc.ProjectStub stub;
    private static final List<String> tagList = new ArrayList<>();

    public ServiceSubscriber(Channel channel) {
        // 'channel' here is a Channel, not a ManagedChannel, so it is not this code's responsibility to
        // shut it down.

        // Passing Channels to code makes code easier to test and makes it easier to reuse Channels.
        blockingStub = ProjectGrpc.newBlockingStub(channel);
        stub = ProjectGrpc.newStub(channel);
    }

    //Grpc method calls

    //Subscrever a tag que escolhemos com o subscriber
    public void subscribeTag(String tag) {
        //Geramos a tag
        Tag requestTag = Tag.newBuilder().setTag(tag).build();
        Iterator<Message> receivedMessages;
        //Tentamos enviar a mensagem, em caso de erro avisa o user
        try {
            receivedMessages = blockingStub.subscribeTo(requestTag);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        //Após inscrevermo-nos numa tag ficamos á espera de mensagens daquela tag
        while (receivedMessages.hasNext()) {
            Message m = receivedMessages.next();
            System.out.println(m.getStamp() + ": " + m.getMessage());
        }
    }

    //Obtemos a lista de tags possíveis do servidor
    public void getTagList() {
        Message request = Message.newBuilder().setMessage("taglist request").build();
        Iterator<Tag> response = blockingStub.getTagList(request);
        while (response.hasNext()) {
            String resp = response.next().getTag();
            tagList.add(resp);
        }
    }


    public void signalKeepAlive(String tag){
        try {
            final Message message = blockingStub.sendAliveSignal(Message.newBuilder().setTag(tag).build());
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
    }

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        // Access a service running on the local machine on port 8080
        String target = "localhost:8080";
        if (args.length > 0) {
            target = args[0];
        }

        // Create a communication channel to the server, known as a Channel. Channels are thread-safe
        // and reusable. It is common to create channels at the beginning of your application and reuse
        // them until the application shuts down.
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();

        ServiceSubscriber subscriber = new ServiceSubscriber(channel);
        subscriber.getTagList();

        System.out.println("Welcome subscriber!\nChoose one of the options:");
        while (true) {
                    System.out.println("1.See Tag List\n2.Subscribe to tag");
                    String second = scanner.next();
                    if (second.equals("1")) {
                        int i = 1;
                        for (String s : tagList) {
                            System.out.println(i + "." + s);
                            i++;
                        }
                        System.out.println();
                    } else if (second.equals("2")) {
                        System.out.println("Insert tag name:");
                        String option = scanner.next();
                        if (!(tagList.contains(option)))
                            System.out.println("Tagname incorrect");
                        else {
                            Thread keepAlive = new Thread(new KeepAliveThread(subscriber,option));
                            keepAlive.start();
                            subscriber.subscribeTag(option);
                            break;
                        }
                    } else System.out.println("Invalid option!");
                }


    }

    static class KeepAliveThread implements Runnable
    {   ServiceSubscriber subscriber;
        String tag;
        public KeepAliveThread(ServiceSubscriber subscriber,String tag) {
            this.subscriber = subscriber;
            this.tag = tag;
        }

        public void run()
        {
            try
            {
                subscriber.signalKeepAlive(tag);
            }
            catch (Exception e)
            {
                // Throwing an exception
                System.out.println ("Exception is caught");
            }
        }
    }
}