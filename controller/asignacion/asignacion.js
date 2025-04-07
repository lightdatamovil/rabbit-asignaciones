import { executeQuery, getDbConfig, getProdDbConfig, updateRedis } from '../../db.js';
import mysql2 from 'mysql2';
import { logCyan, logRed, logYellow } from '../../src/funciones/logsCustom.js';
import { crearTablaAsignaciones } from '../../controller/functions/crearTablaAsignaciones.js';
import { crearUsuario } from '../../controller/functions/crearUsuario.js';
import { insertAsignacionesDB } from '../../controller/functions/insertAsignacionesDB.js';
import { idFromFlexShipment } from '../../controller/functions/idFromFlexShipment.js';
import { idFromNoFlexShipment } from '../../controller/functions/idFromNoFlexShipment.js';
import { crearLog } from '../../src/funciones/crear_log.js';

export async function asignar(startTime, company, userId, body, driverId, deviceFrom) {
    const dbConfig = getProdDbConfig(company);
    const dbConnection = mysql2.createConnection(dbConfig);
    dbConnection.connect();

    const dbConfigLocal = getDbConfig();
    const dbConnectionLocal = mysql2.createConnection(dbConfigLocal);
    dbConnectionLocal.connect();

    try {
        const dataQr = body.dataQr;

        const isFlex = dataQr.hasOwnProperty("sender_id");

        if (isFlex) {
            logCyan("Es Flex");
        } else {
            logCyan("No es Flex");
        }

        const shipmentId = isFlex
            ? await idFromFlexShipment(dataQr.id, dbConnection)
            : await idFromNoFlexShipment(company, dataQr, dbConnection);

        const sqlAsignado = `SELECT id, estado FROM envios_asignaciones WHERE superado=0 AND elim=0 AND didEnvio = ? AND operador = ?`;
        const asignadoRows = await executeQuery(dbConnection, sqlAsignado, [shipmentId, driverId]);

        if (asignadoRows.length > 0) {
            return { feature: "asignacion", estadoRespuesta: false, mensaje: "El paquete ya se encuentra asignado a este chofer." };
        }
        logCyan("El paquete todavia no está asignado");

        const estadoQuery = `SELECT estado FROM envios_historial WHERE superado=0 AND elim=0 AND didEnvio = ?`;
        const estadoRows = await executeQuery(dbConnection, estadoQuery, [shipmentId]);
        logCyan("Obtengo el estado del paquete");

        if (estadoRows.length === 0) {
            throw new Error("No se pudo obtener el estado del paquete.");
        }

        const estado = estadoRows[0].estado;

        await crearTablaAsignaciones(company.did);
        logCyan("Creo la tabla de asignaciones");

        await crearUsuario(company.did);
        logCyan("Creo el usuario");

        const insertSql = `INSERT INTO envios_asignaciones (did, operador, didEnvio, estado, quien, desde) VALUES (?, ?, ?, ?, ?, ?)`;
        const result = await executeQuery(dbConnection, insertSql, ["", driverId, shipmentId, estado, userId, deviceFrom]);
        logCyan("Inserto en la tabla de asignaciones");

        const did = result.insertId;

        const queries = [
            { sql: `UPDATE envios_asignaciones SET did = ? WHERE superado=0 AND elim=0 AND id = ?`, values: [did, did] },
            { sql: `UPDATE envios_asignaciones SET superado = 1 WHERE superado=0 AND elim=0 AND didEnvio = ? AND did != ?`, values: [shipmentId, did] },
            { sql: `UPDATE envios SET choferAsignado = ? WHERE superado=0 AND elim=0 AND did = ?`, values: [driverId, shipmentId] },
            { sql: `UPDATE ruteo_paradas SET superado = 1 WHERE superado=0 AND elim=0 AND didPaquete = ?`, values: [shipmentId] },
            { sql: `UPDATE envios_historial SET didCadete = ? WHERE superado=0 AND elim=0 AND didEnvio = ?`, values: [driverId, shipmentId] },
            { sql: `UPDATE envios SET costoActualizadoChofer = 0 WHERE superado=0 AND elim=0 AND did = ?`, values: [shipmentId] }
        ];

        for (const { sql, values } of queries) {
            await executeQuery(dbConnection, sql, values);
        }
        logCyan("Updateo las tablas");

        await insertAsignacionesDB(dbConnectionLocal, company.did, did, driverId, estado, userId, deviceFrom);
        logCyan("Inserto en la base de datos individual de asignaciones");

        await updateRedis(company.did, shipmentId, driverId);
        logCyan("Actualizo Redis con la asignación");

        const sendDuration = performance.now() - startTime;

        const resultado = { feature: "asignacion", estadoRespuesta: true, mensaje: "Asignación realizada correctamente" };

        crearLog(dbConnectionLocal, company.did, body.userId, body.profile, body, sendDuration.toFixed(2), JSON.stringify(resultado), "asignar", "rabbit", true);

        return resultado;
    } catch (error) {
        const sendDuration = performance.now() - startTime;
        logRed(`Error al asignar paquete:  ${error.stack}`)

        crearLog(dbConnectionLocal, company.did, body.userId, body.profile, body, sendDuration.toFixed(2), error.stack, "asignar", "rabbit", false);

        throw error;
    } finally {
        dbConnection.end();
        dbConnectionLocal.end();
    }
}

