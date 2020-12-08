package service;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;
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

    //Insert needed tags in map
    public static void populate_Map(HashMap<String, List<StreamObserver<Message>>> map) {
        List<StreamObserver<Message>> l = new ArrayList<>();
        map.put("trial", l);
        map.put("license", l);
        map.put("support", l);
        map.put("bug", l);
    }

    public static String getTimeStamp() {
        //Unix seconds
        long unix_seconds = System.currentTimeMillis();
        //convert seconds to milliseconds
        Date date = new Date(unix_seconds);
        // format of the date
        SimpleDateFormat jdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        jdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        String java_date = jdf.format(date);
        System.out.println("\n" + java_date + "\n");

        return java_date;
    }

    static String shortHashCode(String str) {
        int strHashCode = str.hashCode();
        short shorterHashCode = (short) (strHashCode % Short.MAX_VALUE);
        return String.valueOf(shorterHashCode);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final ServiceServer server = new ServiceServer();
        server.start();
        populate_Map(pub_to_sub);
        server.blockUntilShutdown();
    }

    static class ServiceImpl extends ProjectGrpc.ProjectImplBase {

        private static double PoissonCalc(double lambda, Random rng) {
            return (-Math.log(1.0 - rng.nextDouble())/lambda);
        }

        @Override //Done
        public StreamObserver<Message> publish(StreamObserver<Message> responseObserver) {

            return new StreamObserver<Message>() {

                //variaveis de Poisson
                public double LAMBDA = 12;
                public Random RNG = new Random(0);

                @Override
                public void onNext(Message value) {
                    //Calculo de Poisson
                    double time_to_wait = PoissonCalc(LAMBDA, RNG)*3600000;
                    System.out.println((long)time_to_wait);
                    try{
                        Thread.sleep((long)time_to_wait);
                    }catch(RuntimeException | InterruptedException e){
                        System.out.println("Dis dont work nigguh");
                    }
                    //Mensagem com timestamp
                    String time_stamp = getTimeStamp();
                    //Listas utilizadas
                    List<StreamObserver<Message>> l = pub_to_sub.get(value.getTag());
                    List<StreamObserver<Message>> l3 = new ArrayList<>(l);

                    for (int i = 0; i < l.size(); i++) {
                        String id = shortHashCode(l.get(i).toString() + time_stamp);
                        Message newMsg = Message.newBuilder().setMessage(getTimeStamp() + ": " + value.getMessage()).setTag(value.getTag()).build();
                        //Caso o envio da mensagem falhe , significa que o utilizador foi desconectado,
                        // retiramos assim o streamObserver desse cliente
                        try {
                            (l.get(i)).onNext(newMsg);
                            System.out.println("Message sent successfully time_waited="+time_to_wait);
                        } catch (Exception e) {
                            l3.remove(i);
                            //System.out.println("Error");
                        }
                    }
                    //substituimos a lista antiga pela nova lista de clientes que funcionam
                    pub_to_sub.put(value.getTag(), l3);
                }

                @Override
                public void onError(Throwable t) {

                }

                @Override
                public void onCompleted() {

                }
            };
        }

        @Override //Done
        public void publishTo(Tag request, StreamObserver<Message> responseObserver) {
            //verificamos se a tag escolhida é válida
            if (!pub_to_sub.keySet().contains(request.getTag())) {
                responseObserver.onNext(Message.newBuilder().setMessage("Tag is invalid!").build());
                responseObserver.onError(new IOException());
                responseObserver.onCompleted();
            }
            responseObserver.onNext(Message.newBuilder().setMessage("Publisher added to tag").build());
            responseObserver.onCompleted();
        }

        @Override //Not used
        public void getTagList(Message request, StreamObserver<Tag> responseObserver) {
            for (String t : pub_to_sub.keySet()) {
                responseObserver.onNext(Tag.newBuilder().setTag(t).build());
            }
            responseObserver.onCompleted();
        }

        @Override //Not done
        public void sendAliveSignal(Message request, StreamObserver<Message> responseObserver) {
            super.sendAliveSignal(request, responseObserver);
        }

        @Override
        public void subscribeTo(Tag request, StreamObserver<Message> responseObserver) {
            System.out.println(request.getTag());
            System.out.println(responseObserver);
            //Add tag to hashmap
            if (!pub_to_sub.keySet().contains(request.getTag())) {
                responseObserver.onNext(Message.newBuilder().setMessage("Invalid tag , please try again").build());
                responseObserver.onError(new IOException());
                responseObserver.onCompleted();
            } else {
                System.out.println("Updating tag members...");
                List<StreamObserver<Message>> l2 = pub_to_sub.get(request.getTag());
                l2.add(responseObserver);
                System.out.println("Added " + l2.get(0));
                pub_to_sub.put(request.getTag(), l2);
            }
            try {
                responseObserver.onNext(Message.newBuilder().setMessage("You are now in tag:" + request.getTag()).build());
            } catch (Exception e) {

            }
        }
    }
}
