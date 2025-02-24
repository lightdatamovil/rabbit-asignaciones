export async function crearLog(idEmpresa, operador, endpoint, result, quien, idDispositivo, modelo, marca, versionAndroid, versionApp, conLocal) {
    try {
        const fechaunix = Date.now();
        const sqlLog = `INSERT INTO logs (didempresa, quien, cadete, data, fechaunix) VALUES (?, ?, ?, ?, ?)`;

        const values = [idEmpresa, quien, operador, JSON.stringify(result), fechaunix];

        await conLocal.execute(sqlLog, values);
    } catch (error) {
        console.error("Error al crear log:", error);
        throw error;
    }
}
