import { conLocal, redisClient } from './db';
import { escape, createConnection } from 'mysql';



async function crearUsuario(empresa, con) {
    const username = `usuario_${empresa}`;
    const password = '78451296';

    const createUserSql = `CREATE USER IF NOT EXISTS ? IDENTIFIED BY ?`;
    const grantPrivilegesSql = `GRANT ALL PRIVILEGES ON \`asigna_data\`.* TO ?`;

    return new Promise((resolve, reject) => {
        con.query(createUserSql, [username, password], (err) => {
            if (err) {
                return reject({ estado: false, mensaje: "Error al crear el usuario." });
            }
            con.query(grantPrivilegesSql, [username], (err) => {
                if (err) {
                    return reject({ estado: false, mensaje: "Error al otorgar privilegios al usuario." });
                }
                resolve({ estado: true, mensaje: "Usuario creado y privilegios otorgados correctamente." });
            });
        });
    });
}


async function actualizarEmpresas() {
    const empresasDataJson = await redisClient.get('empresas');
    let Aempresas = JSON.parse(empresasDataJson);
    return Aempresas

}


async function crearTablaAsignaciones(empresa, con) {
    const createTableSql = `CREATE TABLE IF NOT EXISTS asignaciones_${empresa} (
        id INT NOT NULL AUTO_INCREMENT,
        didenvio INT NOT NULL,
        chofer INT NOT NULL,
        estado INT NOT NULL DEFAULT '0',
        quien INT NOT NULL,
        autofecha TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        desde INT NOT NULL COMMENT '0 = asignacion / 1 = web',
        superado INT NOT NULL DEFAULT '0',
        elim INT NOT NULL DEFAULT '0',
        PRIMARY KEY (id),
        KEY didenvio (didenvio),
        KEY chofer (chofer),
        KEY autofecha (autofecha)
    ) ENGINE=InnoDB DEFAULT CHARSET=latin1`;

    return new Promise((resolve, reject) => {
        con.query(createTableSql, (err) => {
            if (err) {
                return reject({ estado: false, mensaje: "Error al crear la tabla." });
            }
            resolve();
        });
    });
}

async function guardarDatosEnTabla(empresa, didenvio, chofer, estado, quien, desde, con) {
    const checkSql = `SELECT id FROM asignaciones_${empresa} WHERE didenvio = ${escape(didenvio)} AND superado = 0`;

    return new Promise((resolve, reject) => {
        con.query(checkSql, async (err, rows) => {
            if (err) {
                return reject({ estado: false, mensaje: "Error al verificar la tabla de asignaciones." });
            }

            const Aresult = Object.values(JSON.parse(JSON.stringify(rows)));
            console.log(Aresult, "aaaa");

            if (Aresult.length > 0) {
                const updateSql = `UPDATE asignaciones_${empresa} SET superado = 1 WHERE id = ${Aresult[0].id}`;
                con.query(updateSql, (err) => {
                    if (err) {
                        return reject({ estado: false, mensaje: "Error al actualizar el registro de asignaciones." });
                    }

                    const insertSql = `INSERT INTO asignaciones_${empresa} (didenvio, chofer, estado, quien, desde) VALUES (${escape(didenvio)}, ${escape(chofer)}, ${escape(estado)}, ${escape(quien)}, ${escape(desde)})`;
                    con.query(insertSql, (err) => {
                        if (err) {
                            return reject({ estado: false, mensaje: "Error al insertar en la tabla de asignaciones." });
                        }
                        resolve({ feature: "asignacion", estadoRespuesta: false, mensaje: "Paquete ya asignado." });
                    });
                });
            } else {
                const insertSql = `INSERT INTO asignaciones_${empresa} (didenvio, chofer, estado, quien, desde) VALUES (${escape(didenvio)}, ${escape(chofer)}, ${escape(estado)}, ${escape(quien)}, ${escape(desde)})`;
                con.query(insertSql, (err) => {
                    if (err) {
                        return reject({ feature: "asignacion", estadoRespuesta: false, mensaje: "Paquete ya asignado." });
                    }
                    resolve({ feature: "asignacion", estadoRespuesta: true, mensaje: "Asignado correctamente.", });
                });
            }
        });
    });
}



async function asignar(didenvio, empresa, cadete, quien) {
    const Aempresas = await iniciarProceso();
    const AdataDB = Aempresas[empresa];

    const con = createConnection({
        host: "bhsmysql1.lightdata.com.ar",
        user: AdataDB.dbuser,
        password: AdataDB.dbpass,
        database: AdataDB.dbname
    });

    try {
        await new Promise((resolve, reject) => {
            con.connect(err => {
                if (err) {
                    reject({ estado: false, mensaje: "Error de conexión a la base de datos.", feacture: "asignacion" });
                } else {
                    resolve();
                }
            });
        });

        const sqlAsignado = `SELECT id, estado FROM envios_asignaciones WHERE superado=0 AND elim=0 AND didEnvio = ? AND operador = ?`;
        const rows = await query(con, sqlAsignado, [didenvio, cadete]);

        if (rows.length > 0 && empresa != 4) {
            const did2 = rows[0]["id"];
            const estado2 = rows[0]["estado"];

            const resultadoGuardar = await guardarDatosEnTabla(empresa, did2, cadete, estado2, quien, 0, conLocal);
            return resultadoGuardar;
        }


        const estadoQuery = `SELECT estado FROM envios_historial WHERE superado=0 AND elim=0 AND didEnvio = ?`;
        const estadoRows = await query(con, estadoQuery, [didenvio]);

        if (estadoRows.length === 0) {
            return { estado: false, mensaje: "No se encontraron datos.", feacture: "asignacion" };
        }

        const estado = estadoRows[0]["estado"];

        await crearTablaAsignaciones(empresa, conLocal);
        await crearUsuario(empresa, conLocal);

        const insertSql = `INSERT INTO envios_asignaciones (did, operador, didEnvio, estado, quien, desde) VALUES ("", ?, ?, ?, ?, 'Movil')`;
        const result = await query(con, insertSql, [cadete, didenvio, estado, quien]);

        const did = result.insertId;

        await query(con, `UPDATE envios_asignaciones SET did = ? WHERE superado=0 AND elim=0 AND id = ?`, [did, did]);

        await query(con, `UPDATE envios_asignaciones SET superado = 1 WHERE superado=0 AND elim=0 AND didEnvio = ? AND did != ?`, [didenvio, did]);

        await query(con, `UPDATE envios SET choferAsignado = ? WHERE superado=0 AND elim=0 AND did = ?`, [cadete, didenvio]);

        await query(con, `UPDATE ruteo_paradas SET superado = 1 WHERE superado=0 AND elim=0 AND didPaquete = ?`, [didenvio]);

        await query(con, `UPDATE envios_historial SET didCadete = ? WHERE superado=0 AND elim=0 AND didEnvio = ?`, [cadete, didenvio]);

        await query(con, `UPDATE envios SET costoActualizadoChofer = 0 WHERE superado=0 AND elim=0 AND did = ?`, [didenvio]);

        const resultadoGuardar = await guardarDatosEnTabla(empresa, did, cadete, estado, quien, 0, conLocal);
        return resultadoGuardar;
    } catch (error) {
        console.error("Error en la función asignar:", error);
        return { estado: false, mensaje: "Error en el proceso de asignación.", feacture: "asignacion" };
    } finally {
        con.end();
    }
}


async function desasignar(didenvio, empresa, cadete, quien, response) {
    try {
        const Aempresas = await iniciarProceso();
        const AdataDB = Aempresas[empresa];

        const con = createConnection({
            host: "bhsmysql1.lightdata.com.ar",
            user: AdataDB.dbuser,
            password: AdataDB.dbpass,
            database: AdataDB.dbname
        });

        const sqlOperador = "SELECT operador FROM envios_asignaciones WHERE didEnvio = ? AND superado = 0 AND elim = 0";

        const operadorResult = await new Promise((resolve, reject) => {
            con.query(sqlOperador, [didenvio], (err, result) => {
                if (err) {
                    console.error("Error en la consulta: ", err);
                    return reject({ estadoRespuesta: false, mensaje: "Error en la consulta" });
                }
                resolve(result);
            });
        });

        const operador = operadorResult.length > 0 ? operadorResult[0].operador : -1;
        if (operador === -1 || operador === 0) {
            con.end();
            const responseMessage = { estadoRespuesta: false, mensaje: "El paquete ya está desasignado", feature: "asignacion" };
            return (responseMessage);
        }

        await new Promise((resolve, reject) => {
            let sql = `UPDATE envios_asignaciones SET superado=1 WHERE superado=0 AND elim=0 AND didEnvio = ?`;
            con.query(sql, [didenvio], (err) => {
                if (err) {
                    return reject({ estado: false, mensaje: "Error al desasignar." });
                }
                resolve();
            });
        });

        await new Promise((resolve, reject) => {
            let historialSql = `UPDATE envios_historial SET didCadete=0 WHERE superado=0 AND elim=0 AND didEnvio = ?`;
            con.query(historialSql, [didenvio], (err) => {
                if (err) {
                    return reject({ estado: false, mensaje: "Error al actualizar historial." });
                }
                resolve();
            });
        });

        await new Promise((resolve, reject) => {
            let choferSql = `UPDATE envios SET choferAsignado = 0 WHERE superado=0 AND elim=0 AND did = ?`;
            con.query(choferSql, [didenvio], (err) => {
                if (err) {
                    return reject({ estado: false, mensaje: "Error al desasignar." });
                }
                resolve();
            });
        });

        con.end();
        console.log("Desasignado correctamente.");
        const successResponse = { feature: "asignacion", estadoRespuesta: true, mensaje: "Desasignado correctamente." };
        return (successResponse);

    } catch (error) {
        console.error(error);
        const errorResponse = { estadoRespuesta: false, mensaje: error.mensaje || "Error en el proceso" };
        return (errorResponse);
    }
}




async function iniciarProceso() {
    try {
        await redisClient.connect();

        let empresas = await actualizarEmpresas(Aempresas);

        await redisClient.quit();
        console.log("Conexión a Redis cerrada.");
        return empresas
    } catch (error) {
        console.error("Error en el proceso:", error);
    }
}

let Aempresas = iniciarProceso();
function query(con, sql, params) {
    return new Promise((resolve, reject) => {
        con.query(sql, params, (err, results) => {
            if (err) {
                reject(err);
            } else {
                resolve(results);
            }
        });
    });
}

export default { asignar, desasignar, Aempresas, iniciarProceso, actualizarEmpresas };
