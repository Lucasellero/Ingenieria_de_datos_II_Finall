# Ingenieria_de_datos_II_Finall

package org.example;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

public class App {
    public static void main( String[] args ) {
        String uri = "mongodb://127.0.0.1:27017/";
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase("lab");
            MongoCollection<Document> collection = database.getCollection("historial_clinico");

            SimpleDateFormat fecha = new SimpleDateFormat("yyyy-MM-dd");

            collection.insertOne( new Document("nombre", "Lucas")
                    .append("apellido", "Ellero")
                    .append("nacimiento", fecha.parse("2004-05-02"))
                    .append("documento", "45757343")
                    .append("obra social", "OSDE")
                    .append("seguro", "basico")
                    .append("historia", Arrays.asList(
                            (new Document("tipo", "Estudio")
                                    .append("nombre", "Estudio_clinico")
                                    .append("solicitado_por", "Lucas Ellero")
                                    .append("fecha", fecha.parse("2021-11-09"))
                                    .append("Profesionales", Arrays.asList(new Document("nombre", "Marcos")
                                            .append("appelido", "Gamero")
                                            .append("funcion", "Revisar al paciente"),
                                            new Document("nombre", "Jeronimo")
                                                    .append("apellido", "Fernandez")
                                                    .append("funcion", "Medicar al paciente")))
                                    .append("resultados", "imagen")
                                    .append("costo", 1000)),
                            (new Document("tipo", "Internacion")
                                    .append("estado_inicial", "Grave")
                                    .append("diagnostico", "Operacion urgente")
                                    .append("solicitado_por", "Lucas Ellero")
                                    .append("fecha", fecha.parse("2021-11-09"))
                                    .append("Profesionales", Arrays.asList(new Document("nombre", "Juan")
                                                    .append("appelido", "Martinez")
                                                    .append("funcion", "Encargado de la internacion")))
                                    .append("derivaciones", false))
                                    .append("costo", 6000)))

                    .append("Medicos", Arrays.asList(
                            (new Document("matricula", 1000)
                                    .append("nombre", "Marcos")
                                    .append("apellido", "Gamero")
                                    .append("especialidad", "Medico clinico")),
                            (new Document("matricula", 1001)
                                    .append("nombre", "Jeronimo")
                                    .append("apellido", "Fernandez")
                                    .append("especialidad", "Especialista en medicacion")),
                            (new Document("matricula", 1002)
                                    .append("nombre", "Juan")
                                    .append("apellido", "Martinez")
                                    .append("especialidad", "Internaciones"))
                    ))
                    .append("enfermedades", Arrays.asList("neumonia", "covid-19"))
                    .append("gastos", 7000)
            );

            System.out.println("Paciente agregado correctamente");
            System.out.println("");
            System.out.println("Consulta 1");
            System.out.println("");
            System.out.println("Historias clinicas:");
            collection.find(Filters.and(Filters.eq("documento","45757343"), Filters.elemMatch("historia", Filters.eq("tipo", "Estudio")))).forEach((Document document) ->
            {List<Document> historia = (List<Document>) document.get("historia");
                for (Document historias : historia){
                    String tipo = historias.getString("tipo");
                    String resultado = historias.getString("resultados");
                    if (tipo != "Estudio" && resultado != null){
                        System.out.println("Tipo de historia: " + tipo);
                        System.out.println("Resultado de la historia: " + resultado);
                    }
                }
            });

            System.out.println("");
            System.out.println("Consulta 2");
            System.out.println("");
            System.out.println("Internaciones: ");
            collection.find(Filters.elemMatch("historia", Filters.and(Filters.eq("tipo", "Internacion"), Filters.gte("fecha", fecha.parse("2021-01-01")), Filters.lte("fecha", fecha.parse("2021-12-12"))))).forEach((document -> {
                String nombre = document.getString("nombre");
                String apellido = document.getString("apellido");
                System.out.println("Nombre del paciente: "+nombre);
                System.out.println("Apellido del paciente: "+ apellido);
            }));

            System.out.println("");
            System.out.println("Consulta 3");
            System.out.println("");
            System.out.println("Pacientes: ");
            collection.find(Filters.and(Filters.gte("edad", 40), Filters.regex("nombre", "^M"))).forEach((document -> {
                String nombre = document.getString("nombre");
                String apellido = document.getString("apellido");
                System.out.println("Nombre del paciente: "+nombre);
                System.out.println("Apellido del paciente: "+ apellido);
            }));

            System.out.println("");
            System.out.println("Consulta 4");
            System.out.println("");
            System.out.println("Pacientes: ");
            Document query = new Document();
            query.append("gastos", new Document("$exists", true));
            Document sort = new Document();
            sort.append("gastos", -1);
            Document userWithHighestGastos = collection.find(query)
                    .sort(sort)
                    .limit(1)
                    .first();
            System.out.println(userWithHighestGastos);

            System.out.println("");
            System.out.println("Consulta 5");
            System.out.println("");
            System.out.println("Pacientes: ");
            collection.find(Filters.eq("enfermedades","covid-19")).forEach(document ->
            {String nombre = document.getString("nombre");
            String apellido = document.getString("apellido");
                System.out.println(nombre);
                System.out.println(apellido);});

            System.out.println("");
            System.out.println("Consulta 6");
            System.out.println("");
            System.out.println("Historias del medico Marcos Gamero: ");
            collection.find(Filters.elemMatch("historia", Filters.elemMatch( "Profesionales", Filters.and(Filters.eq("nombre", "Marcos"), Filters.eq("apellido","Gamero"))))).forEach(document ->
            {List<Document> historias = (List<Document>) document.get("historia");
                for (Document historia : historias){
                    List<Document> profesionales = (List<Document>) historia.get("Profesionales");
                    for (Document profesional : profesionales){
                        if ( "Marcos".equals(profesional.getString("nombre")) && "Gamero".equals(profesional.getString("apellido"))){
                            String nombre = historia.getString("nombre");
                            System.out.println(nombre);
                        }
                    }
                }
            });



        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }
}


package org.example;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

public class App {
    public static void main( String[] args ) {
        JedisPool pool = new JedisPool("localhost", 6379);
        try (Jedis jedis = pool.getResource()) {

            HashMap<String, String> candidato1 = new HashMap();
            candidato1.put("codigo", String.valueOf(123));
            candidato1.put("nombre", "Lucas");
            candidato1.put("facultad", "UADE");
            candidato1.put("cantidad_votos", String.valueOf(0));

            jedis.hmset("candidato:10001", candidato1);

            Map<String, String> cand1 = jedis.hgetAll("candidato:10001");
            System.out.println(cand1);

            HashMap<String, String> candidato2 = new HashMap();
            candidato2.put("codigo", String.valueOf(456));
            candidato2.put("nombre", "Matias");
            candidato2.put("facultad", "USAL");
            candidato2.put("cantidad_votos", String.valueOf(0));

            jedis.hmset("candidato:10002", candidato2);

            Map<String, String> cand2 = jedis.hgetAll("candidato:10002");
            System.out.println(cand2);

            HashMap<String, String> candidato3 = new HashMap();
            candidato3.put("codigo", String.valueOf(789));
            candidato3.put("nombre", "Juan Pablo");
            candidato3.put("facultad", "UBA");
            candidato3.put("cantidad_votos", String.valueOf(0));

            jedis.hmset("candidato:10003", candidato3);

            Map<String, String> cand3 = jedis.hgetAll("candidato:10003");
            System.out.println(cand3);

            HashMap<String, String> candidato4 = new HashMap();
            candidato4.put("codigo", String.valueOf(159));
            candidato4.put("nombre", "Facundo");
            candidato4.put("facultad", "UADE");
            candidato4.put("cantidad_votos", String.valueOf(0));

            jedis.hmset("candidato:10004", candidato4);

            Map<String, String> cand4 = jedis.hgetAll("candidato:10004");
            System.out.println(cand4);

            HashMap<String, String> candidato5 = new HashMap();
            candidato5.put("codigo", String.valueOf(472));
            candidato5.put("nombre", "Eduardo");
            candidato5.put("facultad", "UCA");
            candidato5.put("cantidad_votos", String.valueOf(0));

            jedis.hmset("candidato:10005", candidato5);

            Map<String, String> cand5 = jedis.hgetAll("candidato:10005");
            System.out.println(cand5);

            System.out.println("");
            System.out.println("Incrementamos la cantidad de votos");

            jedis.hincrBy("candidato:10001", "cantidad_votos", 13);
            jedis.hincrBy("candidato:10002", "cantidad_votos", 17);
            jedis.hincrBy("candidato:10003", "cantidad_votos", 10);
            jedis.hincrBy("candidato:10004", "cantidad_votos", 20);
            jedis.hincrBy("candidato:10005", "cantidad_votos", 14);

            System.out.println("Cantidad de votos candidato 1: "+ jedis.hget("candidato:10001", "cantidad_votos"));
            System.out.println("Cantidad de votos candidato 2: "+ jedis.hget("candidato:10002", "cantidad_votos"));
            System.out.println("Cantidad de votos candidato 3: "+ jedis.hget("candidato:10003", "cantidad_votos"));
            System.out.println("Cantidad de votos candidato 4: "+ jedis.hget("candidato:10004", "cantidad_votos"));
            System.out.println("Cantidad de votos candidato 5: "+ jedis.hget("candidato:10005", "cantidad_votos"));

            System.out.println("");

            jedis.zadd("votos_candidatos", 13, "Lucas");
            jedis.zadd("votos_candidatos", 17, "Matias");
            jedis.zadd("votos_candidatos", 10, "Juan Pablo");
            jedis.zadd("votos_candidatos", 20, "Facundo");
            jedis.zadd("votos_candidatos", 14, "Eduardo");

            System.out.println("Cantidato con mas votos: ");
            System.out.println(jedis.zrevrange("votos_candidatos", 0, 0));

            System.out.println("");
            System.out.println("Usuarios y sus seguidores: ");

            jedis.sadd("Ruben", "Jorge", "Amelia", "Luisa", "Esteban", "Carlos", "Maria", "Melina", "Eduardo");
            jedis.sadd("Amalia", "Ruben", "Martha", "Megan", "Molly", "Adela", "Oriana");
            jedis.sadd("Julio", "Pablo", "Carolina", "Oriana", "Matias", "Melina", "Adrian", "Acadia");
            jedis.sadd("Lucio", "Jorge", "Martha", "Oriana", "Megan", "Luisa", "Melina");
            System.out.println("Usuario Ruben, seguidores: " + jedis.smembers("Ruben"));
            System.out.println("Usuario Amalia, seguidores: " + jedis.smembers("Amalia"));
            System.out.println("Usuario Julio, seguidores: " +jedis.smembers("Julio"));
            System.out.println("Usaurio Lucio, seguidores: " + jedis.smembers("Lucio"));

            System.out.println("");

            System.out.println("Seguidores en comun de Ruben, Lucio y Julio: ");
            System.out.println(jedis.sinter("Ruben", "Julio", "Lucio"));

            System.out.println("");

            System.out.println("Seguidores de Amalia, pero no de Julio, Ruben y Lucio: ");
            System.out.println(jedis.sdiff("Amalia","Ruben", "Julio", "Lucio"));

        }
    }
}


package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.net.InetSocketAddress;
import java.net.Socket;


public class App {
    public static void main( String[] args ){

        try (CqlSession session = new CqlSessionBuilder()
                .addContactPoint(new InetSocketAddress("127.0.0.1", 9042))
                .withLocalDatacenter("datacenter1").build()) {

            session.execute("CREATE KEYSPACE IF NOT EXISTS repasofinal WITH REPLICATION = {'class' :'SimpleStrategy','replication_factor':'1'};");
            session.execute("use repasofinal;");


            session.execute("Create table consulta1 (patente int, marca text, ano int, multas boolean, dueno boolean, primary key(multas, dueno,patente));");
            session.execute("insert into consulta1 (patente, marca, ano, multas, dueno) values (100, 'peugeot', 2022, true, false);");
            session.execute("insert into consulta1 (patente, marca, ano, multas, dueno) values (101, 'mercedez', 2024, false, true);");
            session.execute("insert into consulta1 (patente, marca, ano, multas, dueno) values (102, 'bmw', 2008, true, false);");
            session.execute("insert into consulta1 (patente, marca, ano, multas, dueno) values (103, 'toyota', 2020, true, true);");
            ResultSet resultado1 = session.execute("select * from consulta1 where multas = true and dueno = false;");
            System.out.println("Consulta 1: ");
            for (Row row : resultado1){
                System.out.println("Patente: "+ row.getInt("patente")
                + "Marca :" + row.getString("marca")
                + "Año :" +row.getInt("ano")
                + "Multas: " + row.getBoolean("multas")
                + "Dueño: "+ row.getBoolean("dueno"));
            }

            System.out.println("");
            System.out.println("Consulta 2: ");
            session.execute("Create table consulta2 (multa int, motivo_multa text, nombre_responsable text, dni_responsable int, patente int, fecha date, primary key(patente, fecha, multa, dni_responsable));");
            session.execute("insert into consulta2 (multa, motivo_multa, nombre_responsable, dni_responsable, patente, fecha) values (1, 'Exceso de valocidad', 'Lucas Ellero', 45757343, 100, '2022-05-10')");
            session.execute("insert into consulta2 (multa, motivo_multa, nombre_responsable, dni_responsable, patente, fecha) values (2, 'Alcolemia', 'Matias Bastian', 46323523, 101, '2022-06-01')");
            session.execute("insert into consulta2 (multa, motivo_multa, nombre_responsable, dni_responsable, patente, fecha) values (3, 'Semaforo en rojo', 'Lucas Ellero', 45757343, 100, '2022-05-24')");
            session.execute("insert into consulta2 (multa, motivo_multa, nombre_responsable, dni_responsable, patente, fecha) values (4, 'Estacionamiento incorrecto', 'Facundo Bel', 442464373, 100, '2022-11-10')");
            ResultSet resultado2 = session.execute("Select * from consulta2 where patente = 100 and fecha >= '2022-01-01' and fecha =< '2022-12-31';");
            for (Row row : resultado2){
                System.out.println("Multas: " + row.getInt("multa")
                + " Motivo multa: " + row.getString("motivo_multa")
                + " Nombre del responsable: " + row.getString("nombre_responsable")
                + " Dni del responsable: " + row.getInt("dni_responsable")
                + " Patente del auto: " + row.getInt("patente")
                + " Fecha de la multa: " + row.getLocalDate("fecha"));
            }

            System.out.println("");
            System.out.println("Consulta 3: ");
            session.execute("Create table consulta3 (patente int, marca text, impuestos boolean, fecha date, primary key(patente, impuestos, fecha));");
            session.execute("Insert into consulta3 (patente, marca, impuestos, fecha) values (100, 'peugeot', true, '2024-07-14');");
            session.execute("Insert into consulta3 (patente, marca, impuestos, fecha) values (101, 'mercedez', false, '2024-07-14');");
            session.execute("Insert into consulta3 (patente, marca, impuestos, fecha) values (102, 'bmw', true, '2024-07-14');");
            session.execute("Insert into consulta3 (patente, marca, impuestos, fecha) values (103, 'toyota', false, '2024-07-11');");
            ResultSet resultado3 = session.execute("Select * from consulta3 where fecha = '2024-07-11' and impuestos = false;");
            for (Row row : resultado3){
                System.out.println("Patente: "+ row.getInt("patente")
                + " Marca: " + row.getString("marca")
                + " Impuestos: " + row.getBoolean("impuestos")
                + " Fecha: " + row.getLocalDate("fecha"));
            }

            System.out.println("");
            System.out.println("Consulta 4: ");
            session.execute("Create table consulta4 (multa int, descuenta_puntos boolean, patente int, marca text, primary key(marca, descuenta_puntos, patente, multa);");
            session.execute("Insert into consulta4 (multa, descuenta_puntos, patente, marca) values (1, true, 100, 'peugeot');");
            session.execute("Insert into consulta4 (multa, descuenta_puntos, patente, marca) values (2, false, 105, 'peugeot');");
            session.execute("Insert into consulta4 (multa, descuenta_puntos, patente, marca) values (3, true, 101, 'mercedez');");
            session.execute("Insert into consulta4 (multa, descuenta_puntos, patente, marca) values (4, true, 107, 'peugeot');");
            ResultSet resultado4 = session.execute("Select * from consulta4 where marca = 'peugeot' and descuenta_puntos = true;");
            for (Row row : resultado4){
                System.out.println("Multa: " + row.getInt("multa")
                + " Descuenta puntos: " + row.getBoolean("descuenta_puntos")
                + " Patente: " + row.getInt("patente")
                + " Marca: " + row.getString("marca"));
            }

            System.out.println("");
            System.out.println("Consulta 5: ");
            session.execute("Create table consulta5 (multa int, motivo_multa text, nombre_responsable text, dni_responsable int, fecha date, primary key(fecha, motivo_multa, multa, dni_responsable));");
            session.execute("insert into consulta5 (multa, motivo_multa, nombre_responsable, dni_responsable, fecha) values (1, 'Exceso de velocidad', 'Lucas Ellero', 45757343, '2022-05-10')");
            session.execute("insert into consulta5 (multa, motivo_multa, nombre_responsable, dni_responsable, fecha) values (2, 'Alcolemia', 'Matias Bastian', 46323523, '2022-06-01')");
            session.execute("insert into consulta5 (multa, motivo_multa, nombre_responsable, dni_responsable, fecha) values (3, 'Semaforo en rojo', 'Lucas Ellero', 45757343, '2022-05-24')");
            session.execute("insert into consulta5 (multa, motivo_multa, nombre_responsable, dni_responsable, fecha) values (5, 'Exceso de velocidad', 'Facundo Bel', 442464373, '2022-11-10')");
            ResultSet resultado5 = session.execute("Select * from consulta5 where fecha > '2022-01-01' and fecha < '2022-12-31' and motivo_multa = 'Exceso de velocidad';");
            for(Row row : resultado5){
                System.out.println("Multa: " + row.getInt("multa")
                + " Motivo de la multa: " + row.getString("motivo_multa")
                + " Nombre del responsable: " + row.getString("nombre_responsable")
                + " Dni del responsable: " + row.getString("dni_responsable")
                + " Fecha: " + row.getLocalDate("fecha"));
            }

            System.out.println("");
            System.out.println("Consulta 6: ");
            session.execute("Create table consulta6 (pagos List<int>, realizo_pago boolean, nombre_dueno text, dni_dueno int, fecha date, primary key (realizo_pago, fecha, dni_dueno));");
            session.execute("insert into consulta6 (pagos, realizo_pago, nombre_dueno, dni_dueno, fecha) values ([1000, 1003] , true, 'Lucas Ellero', 45757343, '2024-07-14')");
            session.execute("insert into consulta6 (pagos, realizo_pago, nombre_dueno, dni_dueno, fecha) values ([1001, 1007] , true, 'Lucas Ellero', 45757343, '2024-07-14')");
            session.execute("insert into consulta6 (pagos, realizo_pago, nombre_dueno, dni_dueno, fecha) values ([1008] , true, 'Lucas Ellero', 45757343, '2024-07-13')");
            session.execute("insert into consulta6 (pagos, realizo_pago, nombre_dueno, dni_dueno, fecha) values ([] , false, 'Lucas Ellero', 45757343, '2024-07-14')");
            ResultSet resultado6 = session.execute("Select * from consulta6 where realizo_pago = true and fecha = '2024-07-14';");
            for(Row row : resultado6){
                System.out.println("Pagos: " + row.getList("pagos", Integer.class)
                        + " ¿Se realizo pagos? " + row.getString("motivo_multa")
                        + " Nombre del dueño: " + row.getString("nombre_responsable")
                        + " Dni del dueño: " + row.getString("dni_responsable")
                        + " Fecha: " + row.getLocalDate("fecha"));
            }
        }

    }
}


package org.example;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Session;

public class App {
    public static void main( String[] args ) {

        final String Uri = "bolt://localhost:7687";
        final String User = "neo4j";
        final String Password = "uade1234";

        try (Driver driver = GraphDatabase.driver(Uri, AuthTokens.basic(User, Password));
             Session session = driver.session()) {
            /*session.run("create (p:Persona {id:1000, nombre:'Lucas', edad:20, genero:'Masculino'})");
            session.run("create (p:Persona {id:1001, nombre:'Edgardo', edad:16, genero:'Masculino'})");
            session.run("create (p:Persona {id:1002, nombre:'Adrian', edad:41, genero:'Masculino'})");
            session.run("create (p:Persona {id:1003, nombre:'Matias', edad:30, genero:'Masculino'})");

            session.run("create (d:Delito {id:900, tipo: 'robo', fecha: date('2023-05-03')})");
            session.run("create (d:Delito {id:901, tipo: 'hurto', fecha: date('2023-02-01')})");

            session.run("create (c:Ciudad {id:1, nombre:'Buenos Aires'})");

            session.run("match (p:Persona {id:1000}), (d:Delito{id:900}) with p, d create (p)-[:cometio{ano:2023}]->(d)");
            session.run("match (p:Persona {id:1003}), (d:Delito{id:901}) with p, d create (p)-[:cometio{ano:2023}]->(d)");

            session.run("match (d:Delito{id:900}), (c:Ciudad {id:1, nombre:'Buenos Aires'}) with d, c create (d)-[:ocurrio]->(c)");
            session.run("match (d:Delito{id:901}), (c:Ciudad {id:1, nombre:'Buenos Aires'}) with d, c create (d)-[:ocurrio]->(c)");*/

            System.out.println("Los delitos que se cometerio en Buenos Aires son: ");
            session.run("match (p:Persona)-[:cometio{ano:2023}]->(d:Delito), (d)-[:ocurrio]->(c:Ciudad{id:1, nombre:'Buenos Aires'}) return d.id as Identificador, d.tipo as Delito").forEachRemaining(record ->
                    System.out.println("Id: " + record.get("Identificador").asInt() + " Delito: "  + record.get("Delito").asString()));

            session.run("o:Objeto{id:800, tipo:'vehiculo', valor:10000})");
            session.run("o:Objeto{id:801, tipo:'mochila', valor:100})");

            session.run("match (o:Objeto{id:800}), (p:Persona {id:1003}) with p, o create (p)-[:robo]->(o)");
            session.run("match (o:Objeto{id:801}), (p:Persona {id:1003}) with p, o create (p)-[:robo]->(o)");

            System.out.println("");
            System.out.println("Objetos que fueron hurtado y quienes lo hierion: ");
            session.run("match (p:Persona)-[:robo]->(o:Objeto), (p)-[:cometio]->(d:Delito) where d.tipo = 'hurto', return o.tipo as objeto, p.nombre as nombre").forEachRemaining(record ->
                System.out.println(record.get("nombre").asString() + " ha robado " + record.get("objeto").asString()));

        }
    }
}
