package service;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ServiceServer {

    private static final Logger logger = Logger.getLogger(ServiceServer.class.getName());

    private Server server;

    private void start() throws IOException {
        /* The port on which the server should run */
        int port = 50052;
        server = ServerBuilder.forPort(port)
                .addService(new ServiceImpl())
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                try {
                    ServiceServer.this.stop();
                } catch (InterruptedException e) {
                    e.printStackTrace(System.err);
                }
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() throws InterruptedException {
        if (server != null) {
            server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    //HashMap de Publisher,Subscriber
    public static HashMap<String, List<StreamObserver<Message>>> pub_to_sub = new HashMap();
    public static HashMap<StreamObserver<Message>, Tag> pub_to_srv = new HashMap();

    public static void main(String[] args) throws IOException, InterruptedException {
        final ServiceServer server = new ServiceServer();
        server.start();
        server.blockUntilShutdown();
    }

    static class ServiceImpl extends ProjectGrpc.ProjectImplBase {

        @Override
        public StreamObserver<Message> publish(StreamObserver<Message> responseObserver) {

            return new StreamObserver<Message>() {
                @Override
                public void onNext(Message value) {
                    List<StreamObserver<Message>> l = pub_to_sub.get(value.getTag());
                    int i =0;
                    for (StreamObserver<Message> so : l) {
                        try {
                            so.onNext(value);
                        } catch (StatusRuntimeException e) {
                            System.out.println("removed unnused position:"+e);
                            l.remove(i);
                        }
                        i++;
                    }
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {

                }
            };
        }

        @Override
        public void publishTo(Tag request, StreamObserver<Message> responseObserver) {
            System.out.println(request.getTag());
            System.out.println(responseObserver);
            List l = new ArrayList();
            pub_to_sub.put(request.getTag(), l);
            responseObserver.onNext(Message.newBuilder().setMessage("Tag added succesfully!").build());
            responseObserver.onCompleted();
        }

        @Override //DONE
        public void getTagList(Message request, StreamObserver<Tag> responseObserver) {
            for (String t : pub_to_sub.keySet()) {
                responseObserver.onNext(Tag.newBuilder().setTag(t).build());
            }
            responseObserver.onCompleted();
        }

        @Override
        public void subscribeTo(Tag request, StreamObserver<Message> responseObserver) {
            System.out.println(request.getTag());
            System.out.println(responseObserver);
            //Add tag to hashmap
            if (pub_to_sub.get(request.getTag()) == null) {
                logger.log(Level.INFO, "Creating Tag ");
                List<StreamObserver<Message>> l1 = new ArrayList<>();
                l1.add(responseObserver);
                pub_to_sub.put(request.getTag(), l1);
            } else {
                System.out.println("Updating tag members...");
                List<StreamObserver<Message>> l2 = pub_to_sub.get(request.getTag());
                l2.add(responseObserver);
                System.out.println(l2.get(0));
                pub_to_sub.put(request.getTag(), l2);
            }
            responseObserver.onNext(Message.newBuilder().setMessage("You are now in tag :" + request).build());


        }
    }
}
