package service;

import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;



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

    //Grpc method calls
    public void subscribeTag(String tag) {
        Tag requestTag = Tag.newBuilder().setTag(tag).build();
        Iterator<Message> receivedMessages;

        try {
            receivedMessages = blockingStub.subscribeTo(requestTag);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }

        while (receivedMessages.hasNext()) {
            System.out.println(receivedMessages.next().getMessage());
        }
    }

    public void getTagList() {
        Message request = Message.newBuilder().setMessage("taglist request").build();
        Iterator<Tag> response;
        response = blockingStub.getTagList(request);
        int i = 1;
        while (response.hasNext()) {
            String resp = response.next().getTag();
            // System.out.println(i + "." + resp);
            i++;
            tagList.add(resp);
        }
    }

    public void chooseTag(String tagname) {
        Tag myTag = Tag.newBuilder().setTag(tagname).build();
        Message response;
        response = blockingStub.publishTo(myTag);
    }

    public void startPublishing(String tag) {
        Scanner scanner = new Scanner(System.in);
        StreamObserver<Message> so = stub.publish(null);
        while (true) {
            String message = scanner.nextLine();
            Message m = Message.newBuilder().setMessage(message).setTag(tag).build();
            so.onNext(m);
        }
    }

    private static List<String> tagList = new ArrayList<>();

    public static void main(String[] args) throws InterruptedException {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Please choose a type of client:\n1.pub -> publisher\n2.sub -> subscriber\n");
        String typeOfUser = scanner.next();

        // Access a service running on the local machine on port 50051
        String target = "localhost:50052";
        if (args.length > 0) {
            target = args[0];
        }

        // Create a communication channel to the server, known as a Channel. Channels are thread-safe
        // and reusable. It is common to create channels at the beginning of your application and reuse
        // them until the application shuts down.
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();

        switch (typeOfUser) {
            case "sub":
            case "subscriber":
            case "2":
                ServiceClient subscriber = new ServiceClient(channel);
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
                            subscriber.subscribeTag(option);
                            break;
                        }
                    } else System.out.println("Invalid option!");
                }
                break;
            case "pub":
            case "publisher":
            case "1":
                ServiceClient publisher = new ServiceClient(channel);
                publisher.getTagList();
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
                        System.out.println("\n");
                    } else if (second.equals("2")) {
                        String myTag = "None";
                        while (!(tagList.contains(myTag))) {
                            System.out.println("Insert tag name:");
                            myTag = scanner.next();
                            if (!(tagList.contains(myTag)))
                                System.out.println("Tagname incorrect");
                            else {
                                publisher.chooseTag(myTag);
                            }
                        }
                        publisher.startPublishing(myTag);
                        break;
                    }
                }
                break;
        }
    }
}