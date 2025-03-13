export async function idFromNoFlexShipment(company, dataQr, dbConnection) {
    const companyIdFromShipment = dataQr.empresa;

    let shipmentId = dataQr.did;
    if (company.did != companyIdFromShipment) {
        try {
            const sql = `SELECT didLocal FROM envios_exteriores WHERE superado=0 AND elim=0 AND didExterno = ? AND didEmpresa = ?`;
            const rows = await executeQuery(dbConnection, sql, [shipmentId, companyIdFromShipment]);

            if (rows.length > 0) {
                shipmentId = rows[0]["didLocal"];
                return shipmentId;
            } else {
                throw new Error("El paquete externo no existe en la logística.");
            }
        } catch (error) {

            logRed(`Error al obtener el id del envío:  ${error.message}`)
            throw error;
        }
    } else {
        return shipmentId;
    }
}
