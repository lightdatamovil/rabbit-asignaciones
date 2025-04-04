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

let connection, channel;

async function setupRabbitMQ() {
    try {
        await redisClient.connect();

        connection = await connect(RABBITMQ_URL);
        channel = await connection.createChannel();

        await channel.assertQueue(QUEUE_NAME_ASIGNACION, { durable: true });
        await channel.assertQueue(QUEUE_NAME_DESASIGNACION, { durable: true });

        logBlue(`[*] Esperando mensajes en la cola "${QUEUE_NAME_ASIGNACION}"`)
        logBlue(`[*] Esperando mensajes en la cola "${QUEUE_NAME_DESASIGNACION}"`)

        channel.consume(QUEUE_NAME_ASIGNACION, async (msg) => {
            if (!msg) return;
            const body = JSON.parse(msg.content.toString());
            try {
                logGreen(`[x] Mensaje recibido: ${JSON.stringify(body)}`);

                const errorMessage = verifyParamaters(body, ['dataQr', 'driverId', 'deviceFrom', 'channel']);
                if (errorMessage) {
                    logRed(`[x] Error al verificar los parámetros: ${errorMessage}`);
                    return;
                }

                const company = await getCompanyById(body.companyId);
                const result = await asignar(company, body.userId, body.dataQr, body.driverId, body.deviceFrom);

                const nowDate = new Date();
                const nowHour = nowDate.toLocaleTimeString();
                const startSendTime = performance.now();

                channel.sendToQueue(body.channel, Buffer.from(JSON.stringify(result)), { persistent: true });

                const sendDuration = performance.now() - startSendTime;
                logGreen(`[x] Respuesta enviada al canal ${body.channel} a las ${nowHour}:`, result);
                logPurple(`Tiempo de envío al canal ${body.channel}: ${sendDuration.toFixed(2)} ms`);
            } catch (error) {
                logRed(`Error al procesar el mensaje: ${error.message}`);
                channel.sendToQueue(body.channel, Buffer.from(JSON.stringify({
                    feature: body.feature,
                    estadoRespuesta: false,
                    mensaje: error.stack,
                    error: true
                })), { persistent: true });
            } finally {
                channel.ack(msg);
            }
        });

        channel.consume(QUEUE_NAME_DESASIGNACION, async (msg) => {
            if (!msg) return;
            const body = JSON.parse(msg.content.toString());
            try {
                logGreen(`[x] Mensaje recibido: ${JSON.stringify(body)}`);
                const errorMessage = verifyParamaters(body, ['dataQr', 'deviceFrom', 'channel']);
                if (errorMessage) {
                    logRed(`[x] Error al verificar los parámetros: ${errorMessage}`);
                    return;
                }

                const company = await getCompanyById(body.companyId);
                const resultado = await desasignar(company, body.userId, body.dataQr, body.deviceFrom);

                const nowDate = new Date();
                const nowHour = nowDate.toLocaleTimeString();
                const startSendTime = performance.now();

                channel.sendToQueue(body.channel, Buffer.from(JSON.stringify(resultado)), { persistent: true });

                const sendDuration = performance.now() - startSendTime;
                logGreen(`[x] Respuesta enviada al canal ${body.channel} a las ${nowHour}: ${JSON.stringify(resultado)}`);
                logPurple(`Tiempo de envío al canal ${body.channel}: ${sendDuration.toFixed(2)} ms`);
            } catch (error) {
                logRed(`Error al procesar el mensaje: ${error.message}`);
                channel.sendToQueue(body.channel, Buffer.from(JSON.stringify({
                    feature: body.feature,
                    estadoRespuesta: false,
                    mensaje: error.stack,
                    error: true
                })), { persistent: true });
            } finally {
                channel.ack(msg);
            }
        });

        // Reconexión automática
        connection.on('close', () => {
            logRed('Conexión cerrada. Reintentando en 5 segundos...');
            setTimeout(connectRabbitMQ, 5000);
        });

        connection.on('error', (err) => {
            logRed('Error en la conexión:', err);
            setTimeout(connectRabbitMQ, 5000);
        });

        channel.on('close', () => {
            logRed('Canal cerrado. Reintentando en 5 segundos...');
            setTimeout(connectRabbitMQ, 5000);
        });

        channel.on('error', (err) => {
            logRed('Error en el canal:', err);
            setTimeout(connectRabbitMQ, 5000);
        });

    } catch (err) {
        logRed(`Error al conectar con RabbitMQ: ${err.message}`);
        setTimeout(connectRabbitMQ, 5000); // Reintento si hay error al conectar
    }
}

async function connectRabbitMQ() {
    if (connection) {
        try { await connection.close(); } catch {}
    }
    if (channel) {
        try { await channel.close(); } catch {}
    }
    await setupRabbitMQ();
}

connectRabbitMQ();
