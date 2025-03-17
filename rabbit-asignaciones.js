import { connect } from 'amqplib';
import dotenv from 'dotenv';
import { asignar, desasignar } from './controller/asignacionesController.js';
import { verifyParamaters } from './src/funciones/verifyParameters.js';
import { getCompanyById, redisClient } from './db.js';
import { logBlue, logGreen, logPurple, logRed } from './src/funciones/logsCustom.js';

dotenv.config({ path: process.env.ENV_FILE || '.env' });

const RABBITMQ_URL = process.env.RABBITMQ_URL;
const QUEUE_NAME_ASIGNACION = process.env.QUEUE_NAME_ASIGNACION;
const QUEUE_NAME_DESASIGNACION = process.env.QUEUE_NAME_DESASIGNACION;

async function connectRabbitMQ() {
    try {
        await redisClient.connect();
        const connection = await connect(RABBITMQ_URL);

        const channel = await connection.createChannel();

        await channel.assertQueue(QUEUE_NAME_ASIGNACION, { durable: true });
        await channel.assertQueue(QUEUE_NAME_DESASIGNACION, { durable: true });

        logBlue(`[*] Esperando mensajes en la cola "${QUEUE_NAME_ASIGNACION}"`)
        logBlue(`[*] Esperando mensajes en la cola "${QUEUE_NAME_DESASIGNACION}"`)

        channel.consume(QUEUE_NAME_ASIGNACION, async (msg) => {
            if (msg !== null) {
                const body = JSON.parse(msg.content.toString());
                try {

                    logGreen(`[x] Mensaje recibido: ${JSON.stringify(body)}`);

                    const errorMessage = verifyParamaters(body, ['dataQr', 'driverId', 'deviceFrom', 'channel']);

                    if (errorMessage) {

                        logRed(`[x] Error al verificar los parámetros:", ${errorMessage}`);
                        return { mensaje: errorMessage };
                    }

                    const company = await getCompanyById(body.companyId);

                    const result = await asignar(company, body.userId, body.dataQr, body.driverId, body.deviceFrom);

                    const nowDate = new Date();
                    const nowHour = nowDate.toLocaleTimeString();

                    const startSendTime = performance.now();

                    channel.sendToQueue(
                        body.channel,
                        Buffer.from(JSON.stringify(result)),
                        { persistent: true }
                    );

                    const endSendTime = performance.now();

                    const sendDuration = endSendTime - startSendTime;


                    logGreen(`[x] Respuesta enviada al canal ${JSON.stringify(body.channel)} a las ${nowHour}: `, result);
                    logPurple(`Tiempo de envío al canal ${body.channel}: ${sendDuration.toFixed(2)} ms`);

                } catch (error) {

                    logRed(`[x] Error al procesar el mensaje:", ${error.menssage}`);
                    let a = channel.sendToQueue(
                        body.channel,
                        Buffer.from(JSON.stringify({ feature: body.feature, estadoRespuesta: false, mensaje: error.stack })),
                        { persistent: true }
                    );
                    if (a) {
                        logGreen(`Mensaje enviado al canal ${body.channel}: { feature: ${body.feature}, estadoRespuesta: false, mensaje: ${error.stack}`);
                    }
                } finally {
                    channel.ack(msg);
                }
            }
        });
        channel.consume(QUEUE_NAME_DESASIGNACION, async (msg) => {
            if (msg !== null) {
                const body = JSON.parse(msg.content.toString());
                try {

                    logGreen(`[x] Mensaje recibido:", ${body}`);

                    const errorMessage = verifyParamaters(body, ['dataQr', 'deviceFrom', 'channel']);

                    if (errorMessage) {

                        logRed(`[x] Error al verificar los parámetros:", ${errorMessage}`);
                        return { mensaje: errorMessage };
                    }

                    const company = await getCompanyById(body.companyId);

                    const resultado = await desasignar(company, body.userId, body.dataQr, body.driverId, body.deviceFrom);
                    logGreen(`[x] Respuesta enviada:", ${JSON.stringify(resultado)}`);

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


                    logGreen(`[x] Respuesta enviada al canal ${body.channel} a las ${nowHour}:`, ` ${resultado}`);

                    logPurple(`Tiempo de envío al canal ${body.channel}: ${sendDuration.toFixed(2)} ms`);
                } catch (error) {
                    logRed(`[x] Error al procesar el mensaje:", ${error.menssage}`)
                    let a = channel.sendToQueue(
                        body.channel,
                        Buffer.from(JSON.stringify({ feature: body.feature, estadoRespuesta: false, mensaje: error.stack })),
                        { persistent: true }
                    );
                    if (a) {
                        logRed(`Mensaje enviado al canal ${body.channel}: { feature: ${body.feature}, estadoRespuesta: false, mensaje: ${error.stack}`);
                    }
                } finally {
                    channel.ack(msg);
                }
            }
        });
    } catch (error) {
        logRed(`Error al conectar con RabbitMQ:", ${error.messaje}`)

    }
}

connectRabbitMQ();
