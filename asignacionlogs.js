const amqp = require('amqplib');
const mysql = require('mysql'); // Asegúrate de tener el paquete mysql instalado

const RABBITMQ_URL = 'amqp://lightdata:QQyfVBKRbw6fBb@158.69.131.226:5672';
const QUEUE_NAME = 'asignacion';

// Conexión a la base de datos MySQL
const conLocal = mysql.createConnection({
    host: "149.56.182.49",
    user: "root",
    password: "585hHtFwZ3icyHBu",
    database: "asigna_data",
    port: 44341
});

// Función para guardar datos en la tabla
async function guardarDatosEnTabla(empresa, didenvio, chofer, estado, quien, desde, con) {
    const checkSql = `SELECT id FROM asignaciones_${empresa} WHERE didenvio = ${mysql.escape(didenvio)} AND superado = 0`;

    return new Promise((resolve, reject) => {
        con.query(checkSql, async (err, rows) => {
            if (err) {
                return reject({ estado: false, mensaje: "Error al verificar la tabla de asignaciones." });
            }

            const Aresult = Object.values(JSON.parse(JSON.stringify(rows)));

            if (Aresult.length > 0) {
                // Si existe, actualizar el campo superado a 1
                const updateSql = `UPDATE asignaciones_${empresa} SET superado = 1 WHERE id = ${Aresult[0].id}`;
                con.query(updateSql, (err) => {
                    if (err) {
                        return reject({ estado: false, mensaje: "Error al actualizar el registro de asignaciones." });
                    }

                    // Insertar un nuevo registro después de actualizar
                    const insertSql = `INSERT INTO asignaciones_${empresa} (didenvio, chofer, estado, quien, desde) VALUES (${mysql.escape(didenvio)}, ${mysql.escape(chofer)}, ${mysql.escape(estado)}, ${mysql.escape(quien)}, ${mysql.escape(desde)})`;
                    con.query(insertSql, (err) => {
                        if (err) {
                            return reject({ estado: false, mensaje: "Error al insertar en la tabla de asignaciones." });
                        }
                        resolve({ feature: "asignacion", estadoRespuesta: false, mensaje: "Paquete ya asignado." });
                    });
                });
            } else {
                // Si no existe, insertar un nuevo registro
                const insertSql = `INSERT INTO asignaciones_${empresa} (didenvio, chofer, estado, quien, desde) VALUES (${mysql.escape(didenvio)}, ${mysql.escape(chofer)}, ${mysql.escape(estado)}, ${mysql.escape(quien)}, ${mysql.escape(desde)})`;
                con.query(insertSql, (err) => {
                    if (err) {
                        return reject({ feature: "asignacion", estadoRespuesta: false, mensaje: "Error al insertar en la tabla de asignaciones." });
                    }
                    resolve({ feature: "asignacion", estadoRespuesta: true, mensaje: "Asignado correctamente." });
                });
            }
        });
    });
}

// Función para escuchar mensajes de RabbitMQ
async function listenToRabbitMQ() {
    try {
        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();

        await channel.assertQueue(QUEUE_NAME, { durable: true });
        await channel.prefetch(25);
        console.log(`Esperando mensajes en la cola: ${QUEUE_NAME}`);

        channel.consume(QUEUE_NAME, async (msg) => {
            if (msg !== null) {
                const messageContent = JSON.parse(msg.content.toString());
                console.log("[x] Mensaje recibido:", messageContent);

                // Extraer el contenido necesario del mensaje
                const { empresa, quien, cadete, dataQR, hora } = messageContent;

                // Guardar en la base de datos
                const fechaunix = Math.floor(Date.now() / 1000); // Obtener la fecha en formato UNIX
                try {
                    await guardarDatosEnTabla(empresa, 1, cadete, "estado", quien, 50, conLocal);
                    const sqlLog = `INSERT INTO logs (didempresa, quien, cadete, data, fechaunix) VALUES (?, ?, ?, ?, ?)`;
                    conLocal.query(sqlLog, [empresa, quien, cadete, JSON.stringify(dataQR), fechaunix], (error, results) => {
                        if (error) {
                            console.error("Error al insertar en la base de datos:", error);
                        } else {
                            console.log("Log guardado en la base de datos:", results.insertId);
                        }
                    });

                    // Enviar un mensaje "hola" al canal
                    const canal = messageContent.canal;
                    const helloMessage = { mensaje: 'hola gonza todo bien?' };
                    channel.sendToQueue(canal, Buffer.from(JSON.stringify(helloMessage)), { persistent: true });
                    console.log(`Mensaje "hola" enviado al canal: ${canal}`);

                    channel.ack(msg); // Confirmar que el mensaje ha sido procesado
                } catch (error) {
                    console.error("Error al guardar datos en la tabla:", error);
                }
            }
        });
    } catch (error) {
        console.error('Error al conectar a RabbitMQ:', error);
    }
}

// Iniciar el listener de RabbitMQ
listenToRabbitMQ();
