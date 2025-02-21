import { Router } from 'express';
import { asignar, desasignar, iniciarProceso, Aempresas } from '../controller/empresaController';
const router = Router();
import { createConnection } from 'mysql';
import { conLocal } from '../db';



router.post('/api/operador', async (req, res) => {
    const dataEntrada = req.body;
    const operador = dataEntrada.operador;


    if (operador === "actualizarEmpresas") {
    } else if (operador === "getEmpresas") {
        return res.status(200).json({ empresas: Aempresas });
    } else {
        await handleOperador(dataEntrada, res);
    }
});

async function handleOperador(dataEntrada, res) {
    const { empresa, cadete, quien, dataQR } = dataEntrada;

    if (empresa == 12 && quien == 49) {
        return sendResponse(res, { estado: false, mensaje: "Comunicarse con la logística." });
    }

    const fechaunix = Date.now();
    const sqlLog = `INSERT INTO logs (didempresa, quien, cadete, data, fechaunix) VALUES (?, ?, ?, ?, ?)`;

    try {
        await conLocal.query(sqlLog, [empresa, quien, cadete, JSON.stringify(dataQR), fechaunix]);

    } catch (err) {
        console.error("Error al insertar en logs:", err);
    }

    try {
        const dataQRParsed = dataQR

        const Aempresas2 = await iniciarProceso()

        if (!Aempresas2[empresa]) {
            return sendResponse(res, { estado: false, mensaje: "No está cargado el ID de la empresa" });
        }

        const AdataDB = Aempresas2[empresa];
        if (!AdataDB.dbname || !AdataDB.dbuser || !AdataDB.dbpass) {
            return sendResponse(res, { estado: false, mensaje: "Error al conectar a la DB" });
        }

        const con = createConnection({
            host: "bhsmysql1.lightdata.com.ar",
            user: AdataDB.dbuser,
            password: AdataDB.dbpass,
            database: AdataDB.dbname
        });

        con.connect(err => {
            if (err) {
                return sendResponse(res, { estado: false, mensaje: err.message });
            }
        });

        const isFlex = dataQRParsed.hasOwnProperty("sender_id");
        const didenvio = isFlex ? 0 : dataQRParsed.did;

        if (!isFlex) {


            handleRegularPackage(didenvio, empresa, cadete, quien, con, res, dataQRParsed);
        } else {
            handleFlexPackage(dataQRParsed.id, con, cadete, empresa, res);
        }
    } catch (error) {
        console.error("Error en el manejo del operador:", error);
        sendResponse(res, { estado: false, mensaje: "Error en el procesamiento de la solicitud." });
    }
}

async function handleRegularPackage(didenvio, empresa, cadete, quien, con, res, dataQRParsed) {
    const didempresapaquete = dataQRParsed.empresa;



    if (empresa != didempresapaquete) {
        const sql = `SELECT didLocal FROM envios_exteriores WHERE superado=0 AND elim=0 AND didExterno = ? AND didEmpresa = ?`;
        try {
            const rows = await query(con, sql, [didenvio, didempresapaquete]);
            const Aresult = rows;

            if (Aresult.length > 0) {
                const didLocal = Aresult[0]["didLocal"];
                cadete !== -2 ? asignar(didLocal, empresa, cadete, quien, res) : desasignar(didLocal, empresa, cadete, quien, res);
            } else {
                return sendResponse(res, { estado: false, mensaje: "El paquete externo no existe en la logística." });
            }
        } catch (err) {
            console.error("Error en consulta de envios_exteriores:", err);
        }
    } else {

        cadete !== -2 ? asignar(didenvio, empresa, cadete, quien, res) : desasignar(didenvio, empresa, cadete, quien, res);
    }
}

function handleFlexPackage(idshipment, con, cadete, empresa, res) {
    const query = `SELECT did FROM envios WHERE flex=1 AND superado=0 AND elim=0 AND ml_shipment_id = ?`;
    con.query(query, [idshipment], (err, rows) => {
        if (err) {
            return sendResponse(res, { estado: false, mensaje: "Error en la consulta de paquete flexible." });
        }

        const Aresult = Object.values(JSON.parse(JSON.stringify(rows)));
        con.end();

        if (Aresult.length > 0) {
            const didenvio = Aresult[0]["did"];
            cadete !== -2 ? asignar(didenvio, empresa, cadete, quien, res) : desasignar(didenvio, empresa, cadete, quien, res);
        } else {
            sendResponse(res, { estado: false, mensaje: "El paquete flexible no se encontró en la base de datos." });
        }
    });
}

function sendResponse(res, response) {
    res.status(200).json(response);
}

function query(connection, sql, params) {
    return new Promise((resolve, reject) => {
        connection.query(sql, params, (error, results) => {
            if (error) return reject(error);
            resolve(results);
        });
    });
}

export default router;




