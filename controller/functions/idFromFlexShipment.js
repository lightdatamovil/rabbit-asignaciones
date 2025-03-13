export async function idFromFlexShipment(shipmentId, dbConnection) {
    try {
        const query = `SELECT did FROM envios WHERE flex=1 AND superado=0 AND elim=0 AND ml_shipment_id = ? LIMIT 1`;
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
