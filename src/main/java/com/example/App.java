package com.example;

import io.debezium.config.Configuration;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.format.ChangeEventFormat;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.File;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class App {
    public static void main(String[] args) throws Exception {

        ConcurrentLinkedQueue<Object> queue = new ConcurrentLinkedQueue<Object>();
        AtomicReference<DebeziumEngine<RecordChangeEvent<SourceRecord>>> engineRef = new AtomicReference<>();
        AtomicReference<Boolean> isEngineStopped = new AtomicReference<>(false);

        File offsetFile = new File("offsets.dat");

        Configuration config = Configuration.create()
                .with("name", "customer-postgres-connector")
                .with("connector.class", "io.debezium.connector.postgresql.PostgresConnector")
                .with("offset.storage", "org.apache.kafka.connect.storage.FileOffsetBackingStore")
                .with("offset.storage.file.filename", offsetFile.getAbsolutePath())
                .with("offset.flush.interval.ms", "6000")
                .with("database.hostname", "localhost")
                .with("database.port", "30028")
                .with("database.user", "test1")
                .with("database.password", "test123")
                .with("database.dbname", "postgres")
                .with("database.server.name", "customer-postgres-db")
                .with("plugin.name", "pgoutput")
                .with("slot.name", "debezium_slot")
                .with("publication.autocreate.mode", "disabled") // ← disable to avoid auto-create error
                .with("table.include.list", "public.t_user") // ← ensure this table exists
                .with("topic.prefix", "customer-postgres")
                .with("offset.flush.interval.ms", "0")
                .build();

        DebeziumEngine<RecordChangeEvent<SourceRecord>> engine = DebeziumEngine
                .create(ChangeEventFormat.of(Connect.class))
                .using(config.asProperties())
                .notifying(record -> {
                    Object k = record.record().key();
                    Object v = record.record().value();
                    System.out.println("Key: " + k + ", Value: " + v);

                    queue.add(k);

                })
                .using((success, message, error) -> {
                    System.err.println("Debezium engine closed. Success = " + success);
                    if (message != null)
                        System.err.println("Message: " + message);
                    if (error != null)
                        error.printStackTrace();
                })
                .build();

        engineRef.set(engine);

        Executor executor = Executors.newFixedThreadPool(10);
        executor.execute(engine);

        System.out.println("Debezium engine started...");

        executor.execute(() -> {
            while (true) {
                if (queue.size() > 0 && isEngineStopped.get()) {
                    System.out.println("Queue is not empty and engine is stopped, processing queue...");
                    try {
                        Thread.sleep(10000);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    queue.clear();
                    System.out.print("Queue processed, restarting engine... ");
                    try {
                        engineRef.get().run();
                        isEngineStopped.set(false);
                        System.out.println("Engine restarted.");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }

            }
        });

        executor.execute(() -> {
            while (true) {

                if (queue.size() >= 2) {
                    System.out.println("Watcher: queue reached " + queue.size() + " → shutting down Debezium.");
                    try {

                        engineRef.get().close();
                        isEngineStopped.set(true);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } 
            }
        });

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                engineRef.get().run();
                ;
                System.out.println("Engine shut down.");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));

        Thread.currentThread().join();
    }
}