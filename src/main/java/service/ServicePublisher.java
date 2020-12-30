package service;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.*;

public class ServicePublisher {
    private final ProjectGrpc.ProjectBlockingStub blockingStub;
    private final ProjectGrpc.ProjectStub stub;
    //Lista de tags existentes
    private static List<String> tagList = new ArrayList<>();

    public ServicePublisher(Channel channel) {
        // 'channel' here is a Channel, not a ManagedChannel, so it is not this code's responsibility to
        // shut it down.

        // Passing Channels to code makes code easier to test and makes it easier to reuse Channels.
        blockingStub = ProjectGrpc.newBlockingStub(channel);
        stub = ProjectGrpc.newStub(channel);
    }

    //Grpc method calls
    //Obter lista de tags do servidor
    public void getTagList() {
        Message request = Message.newBuilder().setMessage("taglist request").build();
        Iterator<Tag> response;
        response = blockingStub.getTagList(request);
        int i = 1;
        while (response.hasNext()) {
            String resp = response.next().getTag();
            i++;
            tagList.add(resp);
        }
    }

    //Escolher a tag em que desejamos publicar
    public void chooseTag(String tagname) {
        Tag myTag = Tag.newBuilder().setTag(tagname).build();
        Message response;
        response = blockingStub.publishTo(myTag);
        System.out.println(response.getStamp()+": "+response.getMessage());
    }

    //Inicia o processo de publishing
    public void startPublishing(String tag) {
        Scanner scanner = new Scanner(System.in);

        //O nosso response observer é null pois não necessitamos dele para o publisher
        StreamObserver<Message> so = stub.publish(null);

        //variáveis de Poisson
        double LAMBDA = 12;
        Random RNG = new Random(0);

        //Enviamos as mensagens pelo Stream Observer criado
        while (true) {
            String message = scanner.nextLine();
            Message m = Message.newBuilder().setMessage(message).setTag(tag).build();
            PoissonCalc(LAMBDA, RNG);
            so.onNext(m);
        }
    }

    // Cálculo de Poisson com o thread sleep incluído
    private static void PoissonCalc(double lambda, Random rng) {
        double time_to_wait = (-Math.log(1.0 - rng.nextDouble()) / lambda);
        time_to_wait *= 3600;
        //wait
        try {
            Thread.sleep((long) time_to_wait);
        } catch (RuntimeException | InterruptedException e) {
            System.out.println("Error not working");
        }
        System.out.println("Message sent successfully time_waited= " + time_to_wait + "ms");
    }

    //Init do cliente
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        // Aceder ao serviço que está a ser executado na porta 8080
        String target = "localhost:8080";
        if (args.length > 0) {
            target = args[0];
        }

        // Create a communication channel to the server, known as a Channel. Channels are thread-safe
        // and reusable. It is common to create channels at the beginning of your application and reuse
        // them until the application shuts down.
        ManagedChannel channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();

        service.ServicePublisher publisher = new service.ServicePublisher(channel);

        //Obtemos a lista de tags possíveis
        publisher.getTagList();

        System.out.println("Welcome publisher!\nChoose one of the options:");
        while (true) {
            System.out.println("1.See Tag List\n2.Publish to tag");
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
                        break;
                    }
                }
                publisher.startPublishing(myTag);
                break;
            }
        }
    }
}


