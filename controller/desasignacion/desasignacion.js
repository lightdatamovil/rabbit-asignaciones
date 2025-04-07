import { executeQuery, getDbConfig, getProdDbConfig, updateRedis } from '../../db.js';
import mysql2 from 'mysql2';
import { logCyan, logRed, logYellow } from '../../src/funciones/logsCustom.js';
import { insertAsignacionesDB } from '../../controller/functions/insertAsignacionesDB.js';
import { idFromFlexShipment } from '../../controller/functions/idFromFlexShipment.js';
import { idFromNoFlexShipment } from '../../controller/functions/idFromNoFlexShipment.js';
import { crearLog } from '../../src/funciones/crear_log.js';

export async function desasignar(startTime, company, userId, body, deviceFrom) {
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

        const sqlOperador = "SELECT operador, estado FROM envios_asignaciones WHERE didEnvio = ? AND superado = 0 AND elim = 0";
        const result = await executeQuery(dbConnection, sqlOperador, [shipmentId]);

        const operador = result.length > 0 ? result[0].operador : 0;

        if (operador == 0) {
            return { feature: "asignacion", estadoRespuesta: false, mensaje: "El paquete ya está desasignado" };
        }
        logCyan("El paquete está asignado");

        if (!shipmentId) {
            throw new Error("No se pudo obtener el id del envío.");
        }

        const insertQuery = "INSERT INTO envios_asignaciones (did, operador, didEnvio, estado, quien, desde) VALUES (?, ?, ?, ?, ?, ?)";
        const resultInsertQuery = await executeQuery(dbConnection, insertQuery, ["", 0, shipmentId, result[0].estado, userId, deviceFrom]);
        logCyan("Inserto en la tabla de asignaciones con el operador 0");

        // Actualizar asignaciones
        await executeQuery(dbConnection, `UPDATE envios_asignaciones SET superado=1, did=${resultInsertQuery.insertId} WHERE superado=0 AND elim=0 AND didEnvio = ?`, [shipmentId]);

        // Actualizar historial
        await executeQuery(dbConnection, `UPDATE envios_historial SET didCadete=0 WHERE superado=0 AND elim=0 AND didEnvio = ?`, [shipmentId]);

        // Desasignar chofer
        await executeQuery(dbConnection, `UPDATE envios SET choferAsignado = 0 WHERE superado=0 AND elim=0 AND did = ?`, [shipmentId]);

        logCyan("Updateo las tablas");

        await insertAsignacionesDB(dbConnectionLocal, company.did, shipmentId, 0, result[0].estado, userId, deviceFrom);
        logCyan("Inserto en la base de datos individual de asignaciones");

        await updateRedis(company.did, shipmentId, 0);
        logCyan("Updateo redis con la desasignación");

        const sendDuration = performance.now() - startTime;

        const resultado = { feature: "asignacion", estadoRespuesta: true, mensaje: "Desasignación realizada correctamente" };

        crearLog(dbConnectionLocal, company.did, body.userId, body.profile, body, sendDuration.toFixed(2), JSON.stringify(resultado), "desasignar", "rabbit", true);

        return resultado;
    } catch (error) {
        const sendDuration = performance.now() - startTime;

        logRed(`Error al desasignar paquete:  ${error.stack}`)

        crearLog(dbConnectionLocal, company.did, userId, body.profile, body, sendDuration.toFixed(2), error.stack, "desasignar", "rabbit", false);

        throw error;
    } finally {
        dbConnection.end();
        dbConnectionLocal.end();
    }
}