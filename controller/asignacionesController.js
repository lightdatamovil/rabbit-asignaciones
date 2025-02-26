import { executeQuery, getDbConfig, getProdDbConfig } from '../db.js';
import mysql2 from 'mysql2';

export async function asignar(company, userId, dataQr, driverId, deviceFrom) {
    const dbConfig = getProdDbConfig(company);
    const dbConnection = mysql2.createConnection(dbConfig);
    dbConnection.connect();

    try {
        const isFlex = dataQr.hasOwnProperty("sender_id");

        const shipmentId = isFlex
            ? await idFromFlexShipment(dataQr.id, dbConnection)
            : await idFromLightdataShipment(company, dataQr, dbConnection);

        const sqlAsignado = `SELECT id, estado FROM envios_asignaciones WHERE superado=0 AND elim=0 AND didEnvio = ? AND operador = ?`;
        const asignadoRows = await executeQuery(dbConnection, sqlAsignado, [shipmentId, driverId]);

        if (asignadoRows.length > 0) {
            return { feature: "asignacion", estadoRespuesta: false, mensaje: "El paquete ya se encuentra asignado a este chofer." };
        }

        const estadoQuery = `SELECT estado FROM envios_historial WHERE superado=0 AND elim=0 AND didEnvio = ?`;

        const estadoRows = await executeQuery(dbConnection, estadoQuery, [shipmentId]);

        if (estadoRows.length === 0) {
            throw new Error("No se pudo obtener el estado del paquete.");
        }

        const estado = estadoRows[0].estado;

        await crearTablaAsignaciones(company.did);
        await crearUsuario(company.did);

        const insertSql = `INSERT INTO envios_asignaciones (did, operador, didEnvio, estado, quien, desde) VALUES (?, ?, ?, ?, ?, ?)`;

        const result = await executeQuery(dbConnection, insertSql, ["", driverId, shipmentId, estado, userId, deviceFrom]);

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

        await insertAsignacionesDB(company.did, did, driverId, estado, userId, deviceFrom);

        return { feature: "asignacion", estadoRespuesta: true, mensaje: "Asignación realizada correctamente" };
    } catch (error) {
        console.error("Error al asignar paquete:", error);
        throw error;
    } finally {
        dbConnection.end();
    }
}

export async function desasignar(company, userId, dataQr, deviceFrom) {
    const dbConfig = getProdDbConfig(company);
    const dbConnection = mysql2.createConnection(dbConfig);
    dbConnection.connect();

    try {
        const isFlex = dataQr.hasOwnProperty("sender_id");

        const shipmentId = isFlex
            ? await idFromFlexShipment(dataQr.id, dbConnection)
            : await idFromLightdataShipment(company, dataQr, dbConnection);

        const sqlOperador = "SELECT operador FROM envios_asignaciones WHERE didEnvio = ? AND superado = 0 AND elim = 0";

        const result = await executeQuery(dbConnection, sqlOperador, [shipmentId]);

        const operador = result.length > 0 ? result[0].operador : 0;

        if (operador == 0) {
            return { feature: "asignacion", estadoRespuesta: false, mensaje: "El paquete ya está desasignado" };
        }

        if (!shipmentId) {
            throw new Error("No se pudo obtener el id del envío.");
        }

        const insertQuery = "INSERT INTO envios_asignaciones (did, operador, didEnvio, estado, quien, desde) VALUES (?, ?, ?, ?, ?, ?)";

        const resultInsertQuery = await executeQuery(dbConnection, insertQuery, ["", 0, shipmentId, 0, userId, deviceFrom]);

        // Actualizar asignaciones
        await executeQuery(dbConnection, `UPDATE envios_asignaciones SET superado=1, did=${resultInsertQuery.insertId} WHERE superado=0 AND elim=0 AND didEnvio = ?`, [shipmentId]);

        // Actualizar historial
        await executeQuery(dbConnection, `UPDATE envios_historial SET didCadete=0 WHERE superado=0 AND elim=0 AND didEnvio = ?`, [shipmentId]);

        // Desasignar chofer
        await executeQuery(dbConnection, `UPDATE envios SET choferAsignado = 0 WHERE superado=0 AND elim=0 AND did = ?`, [shipmentId]);

        return { feature: "asignacion", estadoRespuesta: true, mensaje: "Desasignación realizada correctamente" };
    } catch (error) {
        console.error("Error al desasignar paquete:", error);
        throw error;
    } finally {
        dbConnection.end();
    }
}

async function idFromLightdataShipment(company, dataQr, dbConnection) {
    const companyIdFromShipment = dataQr.empresa;

    const shipmentId = dataQr.did;
    if (company.did != companyIdFromShipment) {
        try {
            const sql = `SELECT didLocal FROM envios_exteriores WHERE superado=0 AND elim=0 AND didExterno = ? AND didEmpresa = ?`;
            const rows = await executeQuery(dbConnection, sql, [companyIdFromShipment, companyIdFromShipment]);

            if (rows.length > 0) {
                shipmentId = rows[0]["didLocal"];
                return shipmentId;
            } else {
                throw new Error("El paquete externo no existe en la logística.");
            }
        } catch (error) {
            console.error("Error al obtener el id del envío:", error);
            throw error;
        }
    } else {
        return shipmentId;
    }
}

async function idFromFlexShipment(shipmentId, dbConnection) {
    try {
        const query = `SELECT did FROM envios WHERE flex=1 AND superado=0 AND elim=0 AND ml_shipment_id = ?`;
        const rows = await executeQuery(dbConnection, query, [shipmentId]);

        if (rows.length > 0) {
            const didenvio = rows[0].did;
            return didenvio;
        } else {
            throw new Error("El paquete flex no se encontró en la base de datos.");
        }
    } catch (error) {
        throw error;
    }
}

async function crearTablaAsignaciones(companyId) {
    const dbConfig = getDbConfig();
    const dbConnection = mysql2.createConnection(dbConfig);
    dbConnection.connect();

    try {
        const createTableSql = `
            CREATE TABLE IF NOT EXISTS asignaciones_${companyId} (
                id INT NOT NULL AUTO_INCREMENT,
                didenvio INT NOT NULL,
                chofer INT NOT NULL,
                estado INT NOT NULL DEFAULT 0,
                quien INT NOT NULL,
                autofecha TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                desde INT NOT NULL COMMENT '0 = asignacion / 1 = web',
                superado INT NOT NULL DEFAULT 0,
                elim INT NOT NULL DEFAULT 0,
                PRIMARY KEY (id),
                KEY didenvio (didenvio),
                KEY chofer (chofer),
                KEY autofecha (autofecha)
            ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
        `;

        await executeQuery(dbConnection, createTableSql);
    } catch (error) {
        console.error("Error al crear la tabla de asignaciones:", error);
        throw error;
    } finally {
        dbConnection.end();
    }
}

async function crearUsuario(companyId) {
    const dbConfig = getDbConfig();
    const dbConnection = mysql2.createConnection(dbConfig);
    dbConnection.connect();

    try {
        const username = `usuario_${companyId}`;
        const password = '78451296';

        const createUserSql = `CREATE USER IF NOT EXISTS ?@'%' IDENTIFIED BY ?`;
        const grantPrivilegesSql = `GRANT ALL PRIVILEGES ON \`asigna_data\`.* TO ?@'%'`;

        await executeQuery(dbConnection, createUserSql, [username, password]);
        await executeQuery(dbConnection, grantPrivilegesSql, [username]);

        return;
    } catch (error) {
        console.error("Error al crear el usuario:", error);
        throw error;
    } finally {
        dbConnection.end();
    }
}

async function insertAsignacionesDB(companyId, shipmentId, driverId, shipmentState, userId, deviceFrom) {
    const dbConfig = getDbConfig();
    const dbConnection = mysql2.createConnection(dbConfig);
    dbConnection.connect();

    try {
        const checkSql = `SELECT id FROM asignaciones_${companyId} WHERE didenvio = ? AND superado = 0`;
        const existingRecords = await executeQuery(dbConnection, checkSql, [shipmentId]);

        if (existingRecords.length > 0) {
            const updateSql = `UPDATE asignaciones_${companyId} SET superado = 1 WHERE id = ?`;
            await executeQuery(dbConnection, updateSql, [existingRecords[0].id]);

            const insertSql = `INSERT INTO asignaciones_${companyId} (didenvio, chofer, estado, quien, desde) VALUES (?, ?, ?, ?, ?)`;
            await executeQuery(dbConnection, insertSql, [shipmentId, driverId, shipmentState, userId, deviceFrom]);
        } else {
            const insertSql = `INSERT INTO asignaciones_${companyId} (didenvio, chofer, estado, quien, desde) VALUES (?, ?, ?, ?, ?)`;
            await executeQuery(dbConnection, insertSql, [shipmentId, driverId, shipmentState, userId, deviceFrom]);
        }
    } catch (error) {
        console.error("Error al insertar asignaciones en la base de datos:", error);
        throw error;
    } finally {
        dbConnection.end();
    }
}
