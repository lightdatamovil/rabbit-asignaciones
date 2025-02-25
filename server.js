import { connect } from 'amqplib';
import dotenv from 'dotenv';
import { asignar, desasignar } from './controller/asignacionesController.js';
import { verifyParamaters } from './src/funciones/verifyParameters.js';
import { getCompanyById, redisClient } from './db.js';

dotenv.config({ path: process.env.ENV_FILE || '.env' });

const RABBITMQ_URL = process.env.RABBITMQ_URL;
const QUEUE_NAME_ASIGNACION = process.env.QUEUE_NAME_ASIGNACION;
const QUEUE_NAME_DESASIGNACION = process.env.QUEUE_NAME_DESASIGNACION;

async function connectRabbitMQ() {
    try {
        await redisClient.connect();
        const startConnectionTime = performance.now();
        const connection = await connect(RABBITMQ_URL);
        const endConnectionTime = performance.now();
        const connectionDuration = endConnectionTime - startConnectionTime;

        const channel = await connection.createChannel();
        await channel.assertQueue(QUEUE_NAME_ASIGNACION, { durable: true });
        await channel.assertQueue(QUEUE_NAME_DESASIGNACION, { durable: true });

        console.log(`[*] Esperando mensajes en la cola "${QUEUE_NAME_ASIGNACION}"`);
        console.log(`Tiempo de conexión a RabbitMQ: ${connectionDuration.toFixed(2)} ms`);
        console.log(`[*] Esperando mensajes en la cola "${QUEUE_NAME_DESASIGNACION}"`);
        console.log(`Tiempo de conexión a RabbitMQ: ${connectionDuration.toFixed(2)} ms`);

        channel.consume(QUEUE_NAME_ASIGNACION, async (msg) => {
            if (msg !== null) {
                try {
                    const body = JSON.parse(msg.content.toString());
                    console.log("[x] Mensaje recibido:", body);

                    const errorMessage = verifyParamaters(body, ['dataQr', 'driverId', 'deviceFrom', 'channel']);

                    if (errorMessage) {
                        console.log("[x] Error al verificar los parámetros:", errorMessage);
                        return { mensaje: errorMessage };
                    }

                    const company = await getCompanyById(body.companyId);

                    let resultado = await asignar(company, body.userId, body.dataQr, body.driverId, body.deviceFrom);

                    const nowDate = new Date();
                    const nowHour = nowDate.toLocaleTimeString();

                    const startSendTime = performance.now();

                    channel.sendToQueue(
                        body.channel,
                        Buffer.from(JSON.stringify(resultado)),
                        { persistent: true }
                    );

                    const endSendTime = performance.now();

                    const sendDuration = endSendTime - startSendTime;

                    console.log(`[x] Respuesta enviada al canal ${body.channel} a las ${nowHour}: `, resultado);
                    console.log(`Tiempo de envío al canal ${body.channel}: ${sendDuration.toFixed(2)} ms`);

                } catch (error) {
                    console.error("[x] Error al procesar el mensaje:", error);
                } finally {
                    channel.ack(msg);
                }
            }
        });
        channel.consume(QUEUE_NAME_DESASIGNACION, async (msg) => {
            if (msg !== null) {
                try {
                    const body = JSON.parse(msg.content.toString());
                    console.log("[x] Mensaje recibido:", body);

                    const errorMessage = verifyParamaters(body, ['dataQr', 'driverId', 'deviceFrom', 'channel']);

                    if (errorMessage) {
                        console.log("[x] Error al verificar los parámetros:", errorMessage);
                        return { mensaje: errorMessage };
                    }

                    const company = await getCompanyById(body.companyId);

                    const resultado = await desasignar(company, body.userId, body.dataQr, body.driverId, body.deviceFrom);

                    const nowDate = new Date();
                    const nowHour = nowDate.toLocaleTimeString();

                    const startSendTime = performance.now();

                    channel.sendToQueue(
                        body.channel,
                        Buffer.from(JSON.stringify(resultado)),
                        { persistent: true }
                    );

                    const endSendTime = performance.now();

                    const sendDuration = endSendTime - startSendTime;

                    console.log(`[x] Respuesta enviada al canal ${body.channel} a las ${nowHour}: `, resultado);
                    console.log(`Tiempo de envío al canal ${body.channel}: ${sendDuration.toFixed(2)} ms`);

                } catch (error) {
                    console.error("[x] Error al procesar el mensaje:", error);
                } finally {
                    channel.ack(msg);
                }
            }
        });
    } catch (error) {
        console.error("Error al conectar con RabbitMQ:", error);
    }
}

connectRabbitMQ();
