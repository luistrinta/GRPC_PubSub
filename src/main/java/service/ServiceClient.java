package service;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.sql.SQLOutput;
import java.util.Iterator;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class ServiceClient {
    private static final Logger logger = Logger.getLogger(ServiceClient.class.getName());
    private final ProjectGrpc.ProjectBlockingStub blockingStub;
    private final ProjectGrpc.ProjectStub stub;

    public ServiceClient(Channel channel) {
        // 'channel' here is a Channel, not a ManagedChannel, so it is not this code's responsibility to
        // shut it down.

        // Passing Channels to code makes code easier to test and makes it easier to reuse Channels.
        blockingStub = ProjectGrpc.newBlockingStub(channel);
        stub = ProjectGrpc.newStub(channel);
    }

    public void subscribeTag(String tag ){
        Tag requestTag = Tag.newBuilder().setTag(tag).build();
        Iterator<Message> receivedMessages;

        try{
            receivedMessages = blockingStub.subscribeTo(requestTag);
        }catch(StatusRuntimeException e){
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }

        while(receivedMessages.hasNext()){
            System.out.println(receivedMessages.next().getMessage());
        }
    }

    public void getTagList(){
      Message request = Message.newBuilder().setMessage("taglist request").build();
      Iterator<Tag> response;

      response = blockingStub.getTagList(request);
      int i =1;
      while(response.hasNext()){
          System.out.println(i+"."+response.next().getTag());
          i++;
      }
    }

    public void chooseTag(String tagname){
        Tag myTag = Tag.newBuilder().setTag(tagname).build();
        Message response;

        response = blockingStub.publishTo(myTag);
    }

    public void startPublishing(String tag){
        Scanner scanner = new Scanner(System.in);
        StreamObserver<Message> so = stub.publish(null);
        while(true){
            String message = scanner.nextLine();
            Message m = Message.newBuilder().setMessage(message).setTag(tag).build();
            so.onNext(m);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Please choose a type of client:\npub -> publisher   sub -> subscriber");
        String typeOfUser = scanner.next();
        System.out.println(typeOfUser);
        // Access a service running on the local machine on port 50051
        String target = "localhost:50052";
        if (args.length > 0) {
            target = args[0];
        }

        // Create a communication channel to the server, known as a Channel. Channels are thread-safe
        // and reusable. It is common to create channels at the beginning of your application and reuse
        // them until the application shuts down.
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();


        //Subscriber uses subscribeTag and ViewTagList
        if("sub".equals(typeOfUser) || "subscriber".equals(typeOfUser)){
            ServiceClient subscriber = new ServiceClient(channel);
            System.out.println("Please write down the tag you want to view:\n 1.View TagList");
            String option = scanner.next();
            switch(option){
                case "1":
                    System.out.println("Tag List:");
                    subscriber.getTagList();
                    break;
                default:
                    subscriber.subscribeTag(option);
                    break;



            }
        }
        //Publisher uses PublishMessageTo
        if("pub".equals(typeOfUser) || "publisher".equals(typeOfUser)){
            ServiceClient publisher = new ServiceClient(channel);
            System.out.println("Please insert your tag:");
            String myTag = scanner.next();
            publisher.chooseTag(myTag);
            System.out.println("Your Tag is "+ myTag);

            publisher.startPublishing(myTag);


        }
    }

}
