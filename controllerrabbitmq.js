const { log } = require('console');
const {conLocal,redisClient} = require('./db');
const http = require('http'); // Asegúrate de importar el módulo http
const mysql = require('mysql');
const qs = require('querystring');
const { features } = require('process');



async function crearUsuario(empresa, con) {
    const username = `usuario_${empresa}`;
    const password = '78451296'; // Cambia esto por una contraseña segura

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
   let   Aempresas = JSON.parse(empresasDataJson);
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
//const guardamos=  await guardarDatosEnTabla(empresa, did, cadete, didenvio, estado, quien, conLocal);

async function guardarDatosEnTabla(empresa, didenvio, chofer, estado, quien, desde, con) {
    // Verificar si ya existe un registro con el mismo didenvio y superado = 0
    const checkSql = `SELECT id FROM asignaciones_${empresa} WHERE didenvio = ${mysql.escape(didenvio)} AND superado = 0`;

    return new Promise((resolve, reject) => {
        con.query(checkSql, async (err, rows) => {
            if (err) {
                return reject({ estado: false, mensaje: "Error al verificar la tabla de asignaciones." });
            }

            const Aresult = Object.values(JSON.parse(JSON.stringify(rows)));
            console.log(Aresult, "aaaa");

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
                        return reject({feature: "asignacion", estadoRespuesta: false, mensaje: "Paquete ya asignado." });
                    }
                    resolve({feature: "asignacion", estadoRespuesta: true, mensaje: "Asignado correctamente.",});
                });
            }
        });
    });
}



async function asignar(didenvio, empresa, cadete, quien) {
    const Aempresas = await iniciarProceso();
    const AdataDB = Aempresas[empresa];

    const con = mysql.createConnection({
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

        // Verificar si el paquete ya está asignado
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

        // Crear la tabla asignaciones_{didempresa} si no existe
        await crearTablaAsignaciones(empresa, conLocal);
        await crearUsuario(empresa, conLocal);

        // Insertar en envios_asignaciones
        const insertSql = `INSERT INTO envios_asignaciones (did, operador, didEnvio, estado, quien, desde) VALUES ("", ?, ?, ?, ?, 'Movil')`;
        const result = await query(con, insertSql, [cadete, didenvio, estado, quien]);

        const did = result.insertId;

        // Actualizar el did en envios_asignaciones
        await query(con, `UPDATE envios_asignaciones SET did = ? WHERE superado=0 AND elim=0 AND id = ?`, [did, did]);

        // Marcar como superado las líneas anteriores
        await query(con, `UPDATE envios_asignaciones SET superado = 1 WHERE superado=0 AND elim=0 AND didEnvio = ? AND did != ?`, [didenvio, did]);

        // Actualizar el chofer asignado
        await query(con, `UPDATE envios SET choferAsignado = ? WHERE superado=0 AND elim=0 AND did = ?`, [cadete, didenvio]);

        // Actualizar ruteo parada
        await query(con, `UPDATE ruteo_paradas SET superado = 1 WHERE superado=0 AND elim=0 AND didPaquete = ?`, [didenvio]);

        // Actualizar envios_historial con el nuevo cadete
        await query(con, `UPDATE envios_historial SET didCadete = ? WHERE superado=0 AND elim=0 AND didEnvio = ?`, [cadete, didenvio]);

        // Actualizar costos chofer
        await query(con, `UPDATE envios SET costoActualizadoChofer = 0 WHERE superado=0 AND elim=0 AND did = ?`, [didenvio]);
      
        // Guardar datos en la tabla asignaciones_{didempresa}
        const resultadoGuardar = await guardarDatosEnTabla(empresa, did, cadete, estado, quien, 0, conLocal);
        return resultadoGuardar; // Devolver el resultado de guardarDatosEnTabla
    } catch (error) {
        console.error("Error en la función asignar:", error);
        return { estado: false, mensaje: "Error en el proceso de asignación.", feacture: "asignacion" };
    } finally {
        con.end(); // Cerrar la conexión a la base de datos
    }
}


async function desasignar(didenvio, empresa, cadete, quien, response) {
    try {
        const Aempresas = await iniciarProceso();
        const AdataDB = Aempresas[empresa];

        const con = mysql.createConnection({
            host: "bhsmysql1.lightdata.com.ar",
            user: AdataDB.dbuser,
            password: AdataDB.dbpass,
            database: AdataDB.dbname
        });

        const sqlOperador = "SELECT operador FROM envios_asignaciones WHERE didEnvio = ? AND superado = 0 AND elim = 0";
        
        // Usamos una promesa para manejar la consulta
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
            const responseMessage = { estadoRespuesta: false, mensaje: "El paquete ya está desasignado",feature: "asignacion"};
            return (responseMessage); // Envía el mensaje a través de RabbitMQ
        }

        // Promesa para realizar la actualización
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
        return (successResponse); // Envía el mensaje a través de RabbitMQ

    } catch (error) {
        console.error(error);
        const errorResponse = { estadoRespuesta: false, mensaje: error.mensaje || "Error en el proceso" };
        return (errorResponse); // Envía el mensaje de error a través de RabbitMQ
    }
}




async function iniciarProceso() {
    try {
        // Conectar a Redis
        await redisClient.connect();

        // Actualizar empresas antes de cerrar la conexión
       let empresas = await actualizarEmpresas(Aempresas);

        // Cerrar la conexión de Redis
        await redisClient.quit();
        console.log("Conexión a Redis cerrada.");
        return empresas
    } catch (error) {
        console.error("Error en el proceso:", error);
    }
}

// Llamar a la función para iniciar el proceso
let Aempresas=  iniciarProceso();
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

module.exports = { asignar,desasignar ,Aempresas,iniciarProceso,actualizarEmpresas};
