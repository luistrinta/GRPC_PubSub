package db;

//Incluimos no build.gradle o compile 'org.mongodb:mongodb-driver-sync:4.0.5'

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import service.ServiceServer;

import java.util.logging.Level;
import java.util.logging.Logger;


public class MyDB {

    final private long T = 1;
    final long UPDATE_TIME = 3600000 * T; //Remove 1 mensagem por hora
    private final MongoCollection<Document> tags;
    private static final Logger logger = Logger.getLogger(ServiceServer.class.getName());

    //Construtor
    public MyDB() {

        //Thread responsável por verificar que a base de dados está atualizada
        Thread messageCleaner = new Thread(new DBMultithread());
        messageCleaner.start();

        MongoClient client = MongoClients.create("mongodb+srv://user:user@cluster0.tzgqs.mongodb.net/<dbname>?retryWrites=true&w=majority");
        MongoDatabase database = client.getDatabase("my_database");

        tags = database.getCollection("tags");
    }

    //Done
    public void addMsg(String tag, String message, int id, long timestamp) {
        Document newMsg = new Document("tag", tag)
                .append("content", message)
                .append("id", id)
                .append("timestamp", timestamp);
        tags.insertOne(newMsg);
    }

    //Done
    public FindIterable<Document> getMessageList(String tag) {
        cleanDB(tag);
        return tags.find(Filters.eq("tag", tag));
    }

    //Limpa todas as mensagens antigas existentes
    public void cleanAll() {
        //Limpa todas as tags que existem na base de dados
        long currentTime = System.currentTimeMillis();
        for (Document d : tags.find()) {
            long messageTime = (long) d.get("timestamp");
            long MESSAGE_TIME = currentTime - messageTime;
            System.out.println(MESSAGE_TIME);
            if (MESSAGE_TIME > UPDATE_TIME) {
                tags.deleteOne(d);
            }
        }
    }

    //Limpa mensagens antigas de uma tag em específico
    public void cleanDB(String tag) {
        long currentTime = System.currentTimeMillis();
        for (Document d : tags.find(Filters.eq("tag", tag))) {
            long messageTime = (long) d.get("timestamp");
            long MESSAGE_TIME = currentTime - messageTime;
            if (MESSAGE_TIME > UPDATE_TIME) {
                tags.deleteOne(d);
            }
        }
    }

    //Classe responsável poelo multithreading
    class DBMultithread implements Runnable {
        @SuppressWarnings({"BusyWait", "InfiniteLoopStatement"})
        public void run() {
            try {
                // Enquanto o servidor estiver a ser executado, temos uma thread que verifica as mensagens
                //na nossa db de acordo com o nosso tempo de atualização especulado
                while (true) {
                    Thread.sleep(UPDATE_TIME);
                    cleanAll();
                    logger.log(Level.WARNING, "Updating database...");
                }
            } catch (Exception e) {
                // Throwing an exception
                System.out.println("Exception is caught");
            }
        }
    }
}

