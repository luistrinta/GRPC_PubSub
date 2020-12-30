package service;

import db.MyDB;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.bson.Document;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ServiceServer {
    public static Random rand = new Random();
    private static final Logger logger = Logger.getLogger(ServiceServer.class.getName());

    private static MyDB mongodb;
    private Server server;

    private void start() throws IOException {
        /* The port on which the server should run */
        int port = 8080;
        server = ServerBuilder.forPort(port)
                .addService(new ServiceImpl())
                .build()
                .start();

        //iniciamos a base de dados em conjunto com o servidor
        mongodb = new MyDB();

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

    //HashMap de tags para subscribers, visto que cada publisher tem apenas 1 tag.
    public static HashMap<String, List<StreamObserver<Message>>> pub_to_sub = new HashMap();

    //Insere tags necessárias ao mapa
    public static void populate_Map(HashMap<String, List<StreamObserver<Message>>> map) {
        map.put("trial", new ArrayList<>());
        map.put("license", new ArrayList<>());
        map.put("support", new ArrayList<>());
        map.put("bug", new ArrayList<>());
    }

    //Obtemos o timestamp
    public static long getTimeStamp() {
        return System.currentTimeMillis();
    }

    //Formatamos o tempo para o formato de dados pretendido
    private static String convertTimeStamp(long time) {
        SimpleDateFormat jdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        jdf.setTimeZone(TimeZone.getTimeZone("GMT"));
        return jdf.format(new Date(time));
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        final ServiceServer server = new ServiceServer();
        server.start();
        populate_Map(pub_to_sub);
        server.blockUntilShutdown();
    }

    static class ServiceImpl extends ProjectGrpc.ProjectImplBase {

        @Override //Começamos a enviar mensagens para um conjunto de stream observers válido
        public StreamObserver<Message> publish(StreamObserver<Message> responseObserver) {

            return new StreamObserver<>() {

                @Override
                public void onNext(Message value) {

                    //Mensagem com timestamp
                    long time_stamp = getTimeStamp();
                    System.out.println(value.getTag());
                    //Listas utilizadas
                    List<StreamObserver<Message>> tag_users = pub_to_sub.get(value.getTag());
                    System.out.println(value.getTag() + " users: " + tag_users.size());
                    List<StreamObserver<Message>> valid_users = new ArrayList<>(tag_users);

                    //id da mensagem(gerado aleatóriamente)
                    int id = rand.nextInt(100000);

                    //Adicionamos a mensagem á base de dados
                    mongodb.addMsg(value.getTag(), value.getMessage(), id, time_stamp);

                    //Criação de uma mensagem nova com o timestamp no formato correto
                    String converted_time_stamp = convertTimeStamp(time_stamp);

                    Message newMsg = Message.newBuilder()
                            .setMessage(value.getMessage())
                            .setTag(value.getTag())
                            .setStamp(converted_time_stamp)
                            .setId(id)
                            .build();

                    for (int i = 0; i < tag_users.size(); i++) {
                        //Caso o envio da mensagem falhe , significa que o utilizador foi desconectado,
                        // retiramos assim o streamObserver desse cliente
                        try {
                            (tag_users.get(i)).onNext(newMsg);
                        } catch (RuntimeException e) {
                            valid_users.remove(i);
                        }
                    }
                    //substituimos a lista antiga pela nova lista de clientes que estão ativos
                    pub_to_sub.put(value.getTag(), valid_users);
                }

                @Override
                public void onError(Throwable t) {
                }

                @Override
                public void onCompleted() {
                }
            };
        }

        @Override //Verificamos que se pretende publicar é válida
        public void publishTo(Tag request, StreamObserver<Message> responseObserver) {
            //verificamos se a tag escolhida é válida
            if (!pub_to_sub.containsKey(request.getTag())) {
                responseObserver.onNext(Message.newBuilder().setMessage("Tag is invalid!").build());
                responseObserver.onError(new IOException());
                responseObserver.onCompleted();
            }
            responseObserver.onNext(Message.newBuilder().setMessage("You are now publishing to " + request.getTag()).setStamp("server").build());
            responseObserver.onCompleted();
        }

        @Override //Obtemos a lista de tags possíveis
        public void getTagList(Message request, StreamObserver<Tag> responseObserver) {
            //percorremos o key set e retornamos as chaves(tags possiveis)
            for (String t : pub_to_sub.keySet()) {
                responseObserver.onNext(Tag.newBuilder().setTag(t).build());
            }
            responseObserver.onCompleted();
        }

        @Override //Not done
        public void sendAliveSignal(Message request, StreamObserver<Message> responseObserver) {

        }

        @Override // Adicionamos o stream observer ao sua respectiva tag , caso esta seja válida
        public void subscribeTo(Tag request, StreamObserver<Message> responseObserver) {

            //Verificamos se o keyset contem a tag escrita, se contiver procede , caso não contenha dá erro
            if (!pub_to_sub.containsKey(request.getTag())) {
                responseObserver.onNext(Message.newBuilder().setMessage("Invalid tag , please try again").build());
                responseObserver.onError(new IOException());
                responseObserver.onCompleted();
            } else {
                logger.log(Level.INFO, "Updating tag members");
                List<StreamObserver<Message>> tag_members = pub_to_sub.get(request.getTag());
                tag_members.add(responseObserver);
                pub_to_sub.put(request.getTag(), tag_members);
                try {
                    responseObserver.onNext(Message.newBuilder().setMessage("You are now in tag " + request.getTag()).setStamp("server").build());
                    sendOldMessages(request.getTag(), responseObserver);
                } catch (Exception e) {
                    logger.log(Level.WARNING, "Error while processing data");
                }
            }
        }

        private static void sendOldMessages(String tag, StreamObserver<Message> observer) {
            for (Document d : mongodb.getMessageList(tag)) {
                String correct_time = convertTimeStamp((long) d.get("timestamp"));
                observer.onNext(Message.newBuilder().setMessage((String) d.get("content")).setStamp(correct_time).build());
            }
        }
    }
}
