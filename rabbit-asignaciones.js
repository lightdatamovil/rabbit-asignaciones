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

let connection = null;
let channel = null;
let reconnecting = false;

async function startRabbitMQ() {
    try {
        if (!redisClient.isOpen) {
            await redisClient.connect();
        }

        connection = await connect(RABBITMQ_URL);
        channel = await connection.createChannel();

        await channel.assertQueue(QUEUE_NAME_ASIGNACION, { durable: true });
        await channel.assertQueue(QUEUE_NAME_DESASIGNACION, { durable: true });

        logBlue(`[*] Conectado. Esperando mensajes en "${QUEUE_NAME_ASIGNACION}" y "${QUEUE_NAME_DESASIGNACION}"`);

        channel.consume(QUEUE_NAME_ASIGNACION, async (msg) => {
            if (!msg) return;
            const body = JSON.parse(msg.content.toString());
            try {
                logGreen(`[x] Mensaje recibido ASIGNACION: ${JSON.stringify(body)}`);

                const errorMessage = verifyParamaters(body, ['dataQr', 'driverId', 'deviceFrom', 'channel']);
                if (errorMessage) {
                    logRed(`[x] Error parámetros: ${errorMessage}`);
                    return;
                }

                const company = await getCompanyById(body.companyId);
                const result = await asignar(company, body.userId, body.dataQr, body.driverId, body.deviceFrom);

                const startSendTime = performance.now();
                channel.sendToQueue(body.channel, Buffer.from(JSON.stringify(result)), { persistent: true });
                const sendDuration = performance.now() - startSendTime;

                logGreen(`[x] Respuesta enviada a ${body.channel}:`, result);
                logPurple(`⏱ Tiempo envío: ${sendDuration.toFixed(2)} ms`);
            } catch (error) {
                logRed(`❌ Error al procesar mensaje: ${error.message}`);
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
                logGreen(`[x] Mensaje recibido DESASIGNACION: ${JSON.stringify(body)}`);

                const errorMessage = verifyParamaters(body, ['dataQr', 'deviceFrom', 'channel']);
                if (errorMessage) {
                    logRed(`[x] Error parámetros: ${errorMessage}`);
                    return;
                }

                const company = await getCompanyById(body.companyId);
                const result = await desasignar(company, body.userId, body.dataQr, body.deviceFrom);

                const startSendTime = performance.now();
                channel.sendToQueue(body.channel, Buffer.from(JSON.stringify(result)), { persistent: true });
                const sendDuration = performance.now() - startSendTime;

                logGreen(`[x] Respuesta enviada a ${body.channel}: ${JSON.stringify(result)}`);
                logPurple(`⏱ Tiempo envío: ${sendDuration.toFixed(2)} ms`);
            } catch (error) {
                logRed(`❌ Error al procesar mensaje: ${error.message}`);
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

        connection.on('close', handleReconnect);
        connection.on('error', (err) => {
            logRed(`⚠️ Conexión con error: ${err.message}`);
        });

        channel.on('close', handleReconnect);
        channel.on('error', (err) => {
            logRed(`⚠️ Canal con error: ${err.message}`);
        });

    } catch (err) {
        logRed(`❌ Error inicial al conectar: ${err.message}`);
        handleReconnect();
    }
}

function handleReconnect() {
    if (reconnecting) return;
    reconnecting = true;

    logRed('🔄 Intentando reconectar en 5 segundos...');
    setTimeout(async () => {
        reconnecting = false;
        try {
            if (connection) {
                await connection.close().catch(() => {});
            }
            if (channel) {
                await channel.close().catch(() => {});
            }
        } catch (e) {
            logRed('❌ Error cerrando conexión/canal antes de reconectar.');
        }
        await startRabbitMQ();
    }, 5000);
}

startRabbitMQ();
